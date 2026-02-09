"""FastAPI application — receives PDFs, extracts data via Gemini, returns XLSX."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import Response

from .consolidator import consolidate, deduplicate
from .excel_writer import build_xlsx
from .gemini_extractor import extract_page
from .pdf_splitter import split_pdf_to_pages

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
    files: list[UploadFile] = File(..., description="One or more PDF files"),
) -> Response:
    """Main extraction endpoint.

    Flow:
    1. Read all uploaded PDFs in order.
    2. Split each PDF into single-page PDFs (preserving order).
    3. Send each page to Gemini sequentially (to preserve order and avoid rate limits).
    4. Consolidate and deduplicate results.
    5. Generate XLSX and return it.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No PDF files uploaded.")

    logger.info("Received %d PDF file(s)", len(files))

    # ── Step 1+2: Read and split ──────────────────────────────────────────
    all_pages: list[tuple[bytes, str]] = []  # (page_bytes, label)

    for file_idx, upload in enumerate(files, start=1):
        filename = upload.filename or f"file{file_idx}"
        content_type = upload.content_type or ""
        if "pdf" not in content_type and not filename.lower().endswith(".pdf"):
            raise HTTPException(
                status_code=400,
                detail=f"File '{filename}' is not a PDF (content-type: {content_type}).",
            )

        pdf_bytes = await upload.read()
        logger.info("File %d (%s): %d bytes", file_idx, filename, len(pdf_bytes))

        pages = split_pdf_to_pages(pdf_bytes)
        logger.info("File %d split into %d pages", file_idx, len(pages))

        for page_num, page_bytes in enumerate(pages, start=1):
            label = f"PDF{file_idx}-P{page_num}"
            all_pages.append((page_bytes, label))

    logger.info("Total pages to process: %d", len(all_pages))

    # ── Step 3: Extract via Gemini (sequential to preserve order) ─────────
    extractions: list[dict] = []
    labels: list[str] = []

    for page_bytes, label in all_pages:
        logger.info("Processing %s …", label)
        t0 = time.time()
        result = extract_page(page_bytes, page_label=label)
        elapsed = time.time() - t0
        logger.info("Extracted %s in %.1fs (%d rows)", label, elapsed, len(result.get("rows", [])))
        extractions.append(result)
        labels.append(label)

    # ── Step 4: Consolidate ───────────────────────────────────────────────
    rows = consolidate(extractions, labels)
    logger.info("Consolidated: %d rows before dedup", len(rows))

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
