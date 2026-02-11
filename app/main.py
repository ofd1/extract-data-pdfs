"""FastAPI application — receives PDFs, extracts data via Gemini, returns XLSX."""

from __future__ import annotations

import io
import logging
import os
import time
import zipfile
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import Response

from .consolidator import consolidate, deduplicate
from .excel_writer import build_xlsx
from .gemini_extractor import build_context_summary, extract_page
from .mascara_generator import gerar_mascaras, verificar_mascaras
from .pdf_splitter import split_pdf_to_pages
from .validators import validar_extracao

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="PDF Accounting Data Extractor",
    description="Receives accounting PDFs, extracts balance-sheet data via Gemini AI, returns consolidated XLSX.",
    version="1.0.0",
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/extract")
async def extract_endpoint(
    files: list[UploadFile] = File(..., description="One or more PDF or ZIP files"),
) -> Response:
    """Main extraction endpoint.

    Accepts PDF files and/or ZIP archives containing PDFs.
    PDFs inside a ZIP are sorted alphabetically to guarantee order.

    Flow:
    1. Read uploads — extract PDFs from ZIPs if needed.
    2. Split each PDF into single-page PDFs (preserving order).
    3. Send each page to Gemini sequentially (to preserve order and avoid rate limits).
    4. Consolidate and deduplicate results.
    5. Generate XLSX and return it.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No PDF files uploaded.")

    logger.info("Received %d file(s)", len(files))

    # ── Step 1+2: Read uploads, unzip if needed, and split ────────────────
    # Build ordered list of (pdf_bytes, source_name) from uploads.
    # ZIP files are extracted; PDFs sorted alphabetically within each ZIP.
    pdf_inputs: list[tuple[bytes, str]] = []

    for upload in files:
        raw = await upload.read()
        filename = upload.filename or "unknown"
        content_type = upload.content_type or ""

        is_zip = (
            filename.lower().endswith(".zip")
            or "zip" in content_type
        )

        if is_zip:
            try:
                zf = zipfile.ZipFile(io.BytesIO(raw))
            except zipfile.BadZipFile:
                raise HTTPException(status_code=400, detail=f"File '{filename}' is not a valid ZIP.")
            # Sort entries alphabetically to guarantee deterministic order
            pdf_names = sorted(
                n for n in zf.namelist()
                if n.lower().endswith(".pdf") and not n.startswith("__MACOSX")
            )
            if not pdf_names:
                raise HTTPException(status_code=400, detail=f"ZIP '{filename}' contains no PDF files.")
            logger.info("ZIP '%s': found %d PDF(s)", filename, len(pdf_names))
            for name in pdf_names:
                pdf_inputs.append((zf.read(name), name))
            zf.close()
        elif "pdf" in content_type or filename.lower().endswith(".pdf"):
            pdf_inputs.append((raw, filename))
        else:
            raise HTTPException(
                status_code=400,
                detail=f"File '{filename}' is not a PDF or ZIP (content-type: {content_type}).",
            )

    if not pdf_inputs:
        raise HTTPException(status_code=400, detail="No PDF files found in the upload.")

    # Split each PDF into single pages
    all_pages: list[tuple[bytes, str]] = []  # (page_bytes, label)

    for file_idx, (pdf_bytes, source_name) in enumerate(pdf_inputs, start=1):
        logger.info("File %d (%s): %d bytes", file_idx, source_name, len(pdf_bytes))

        pages = split_pdf_to_pages(pdf_bytes)
        logger.info("File %d split into %d pages", file_idx, len(pages))

        for page_num, page_bytes in enumerate(pages, start=1):
            label = f"PDF{file_idx}-P{page_num}"
            all_pages.append((page_bytes, label))

    logger.info("Total pages to process: %d", len(all_pages))

    # ── Step 3: Extract via Gemini (sequential to preserve order) ─────────
    extractions: list[dict] = []
    labels: list[str] = []
    contexto_anterior = ""

    for page_bytes, label in all_pages:
        logger.info("Processing %s …", label)
        t0 = time.time()
        result = extract_page(
            page_bytes,
            page_label=label,
            contexto_anterior=contexto_anterior,
        )
        elapsed = time.time() - t0

        # Validate extraction
        is_valid, warnings = validar_extracao(result, label)
        logger.info(
            "Extracted %s in %.1fs — type=%s, periodo=%s, %d rows, %d warnings",
            label, elapsed,
            result.get("type", "?"),
            result.get("periodo", "?"),
            len(result.get("rows", [])),
            len(warnings),
        )

        if is_valid:
            extractions.append(result)
            labels.append(label)
            contexto_anterior = build_context_summary(result)
        else:
            logger.error("Skipping %s — structurally invalid extraction", label)

    # ── Step 4: Consolidate ───────────────────────────────────────────────
    rows = consolidate(extractions, labels)
    logger.info("Consolidated: %d rows before dedup", len(rows))

    # Check and generate missing accounting masks
    all_have_masks, missing_count = verificar_mascaras(rows)
    if not all_have_masks:
        logger.info("Generating masks for %d rows…", missing_count)
        rows = gerar_mascaras(rows)

    rows = deduplicate(rows)
    logger.info("After dedup: %d rows", len(rows))

    # ── Step 5: Generate XLSX ─────────────────────────────────────────────
    xlsx_bytes = build_xlsx(rows)
    logger.info("XLSX generated: %d bytes", len(xlsx_bytes))

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": 'attachment; filename="balancete_consolidado.xlsx"',
        },
    )
