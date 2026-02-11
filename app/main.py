"""FastAPI application — receives PDFs, extracts data via Gemini, returns XLSX."""

from __future__ import annotations

import io
import logging
import os
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

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

MASK_GENERATION_THRESHOLD = 0.80  # Only auto-generate masks if ≥80% are missing


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _process_pdf_pages(
    pages: list[tuple[bytes, str]],
) -> tuple[list[dict], list[str], list[dict]]:
    """Process all pages of a single PDF sequentially.

    Pages are processed in order so that page context (previous page summary)
    can be passed to the next page for continuity.

    Args:
        pages: List of (page_bytes, label) for one PDF, in page order.

    Returns:
        (extractions, labels, errors) for this PDF.
    """
    extractions: list[dict] = []
    labels: list[str] = []
    errors: list[dict] = []
    contexto_anterior = ""
    locked_periodo = ""

    for page_bytes, label in pages:
        logger.info("Processing %s …", label)
        t0 = time.time()

        try:
            result = extract_page(
                page_bytes,
                page_label=label,
                contexto_anterior=contexto_anterior,
            )
        except Exception as exc:
            elapsed = time.time() - t0
            logger.error("Error extracting %s after %.1fs: %s", label, elapsed, exc)
            errors.append({"pagina": label, "erro": str(exc)})
            continue

        elapsed = time.time() - t0

        # Lock periodo from the first page of this PDF
        periodo_raw = str(result.get("periodo", "")).strip()
        if not locked_periodo and periodo_raw:
            locked_periodo = periodo_raw
            logger.info("Periodo locked for this PDF: %s", locked_periodo)
        if locked_periodo:
            result["periodo"] = locked_periodo

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
            errors.append({"pagina": label, "erro": "Extração estruturalmente inválida"})

    return extractions, labels, errors


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
    3. Process PDFs in parallel (pages within each PDF are sequential).
    4. Consolidate and deduplicate results.
    5. Generate XLSX and return it.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No PDF files uploaded.")

    logger.info("Received %d file(s)", len(files))

    # ── Step 1+2: Read uploads, unzip if needed, and split ────────────────
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

    # ── Split each PDF and group pages by PDF ─────────────────────────────
    pdf_groups: list[list[tuple[bytes, str]]] = []  # one group per PDF

    for file_idx, (pdf_bytes, source_name) in enumerate(pdf_inputs, start=1):
        logger.info("File %d (%s): %d bytes", file_idx, source_name, len(pdf_bytes))

        pages = split_pdf_to_pages(pdf_bytes)
        logger.info("File %d split into %d pages", file_idx, len(pages))

        group: list[tuple[bytes, str]] = []
        for page_num, page_bytes in enumerate(pages, start=1):
            label = f"PDF{file_idx}-P{page_num}"
            group.append((page_bytes, label))
        pdf_groups.append(group)

    total_pages = sum(len(g) for g in pdf_groups)
    logger.info("Total pages to process: %d across %d PDF(s)", total_pages, len(pdf_groups))

    # ── Step 3: Extract — parallel per PDF, sequential per page ───────────
    all_extractions: list[dict] = []
    all_labels: list[str] = []
    all_errors: list[dict] = []

    max_workers = min(len(pdf_groups), 4)
    if max_workers <= 1:
        # Single PDF — no need for thread pool overhead
        ext, lab, err = _process_pdf_pages(pdf_groups[0])
        all_extractions.extend(ext)
        all_labels.extend(lab)
        all_errors.extend(err)
    else:
        logger.info("Processing %d PDFs in parallel (max_workers=%d)", len(pdf_groups), max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_process_pdf_pages, group)
                for group in pdf_groups
            ]
            # Collect in original PDF order
            for future in futures:
                ext, lab, err = future.result()
                all_extractions.extend(ext)
                all_labels.extend(lab)
                all_errors.extend(err)

    if all_errors:
        logger.warning("Extraction completed with %d error(s)", len(all_errors))
        for e in all_errors:
            logger.warning("  %s: %s", e["pagina"], e["erro"])

    # ── Step 4: Consolidate ───────────────────────────────────────────────
    rows = consolidate(all_extractions, all_labels)
    logger.info("Consolidated: %d rows before dedup", len(rows))

    # Smart mask threshold: only generate if ≥80% are missing
    all_have_masks, missing_count = verificar_mascaras(rows)
    if all_have_masks:
        logger.info("All rows have masks — skipping generation")
    elif rows:
        ratio = missing_count / len(rows)
        if ratio >= MASK_GENERATION_THRESHOLD:
            logger.info(
                "%d/%d rows missing masks (%.0f%%) — generating via AI",
                missing_count, len(rows), ratio * 100,
            )
            rows = gerar_mascaras(rows)
        else:
            logger.info(
                "Only %d/%d rows missing masks (%.0f%%) — skipping AI mask generation (threshold: %.0f%%)",
                missing_count, len(rows), ratio * 100, MASK_GENERATION_THRESHOLD * 100,
            )

    rows = deduplicate(rows)
    logger.info("After dedup: %d rows", len(rows))

    # ── Step 5: Generate XLSX ─────────────────────────────────────────────
    xlsx_bytes = build_xlsx(rows, errors=all_errors)
    logger.info("XLSX generated: %d bytes", len(xlsx_bytes))

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": 'attachment; filename="balancete_consolidado.xlsx"',
        },
    )
