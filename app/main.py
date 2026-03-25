"""FastAPI application — receives PDFs, extracts data via Gemini, returns XLSX."""

from __future__ import annotations

import asyncio
import io
import logging
import os
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from .consolidator import consolidate, deduplicate
from .excel_writer import build_xlsx
from .gemini_extractor import build_context_summary, extract_page
from .mascara_generator import gerar_mascaras, verificar_mascaras
from .pdf_splitter import split_pdf_to_pages
from .validators import validar_extracao
from .arithmetic_validator import validar_aritmetica
from .classifier import classificar_contas

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MASK_GENERATION_THRESHOLD = 0.80  # Only auto-generate masks if >=80% are missing

# ---------------------------------------------------------------------------
# In-memory job store
# ---------------------------------------------------------------------------
jobs: dict[str, dict] = {}

JOB_TTL_SECONDS = 30 * 60  # 30 minutes


def _cleanup_old_jobs() -> None:
    """Remove jobs older than JOB_TTL_SECONDS."""
    now = time.time()
    expired = [jid for jid, j in jobs.items() if now - j["created_at"] > JOB_TTL_SECONDS]
    for jid in expired:
        del jobs[jid]
    if expired:
        logger.info("Cleaned up %d expired job(s)", len(expired))


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(content={
        "status": job["status"],
        "progress": job["progress"],
        "message": job["message"],
        "error": job.get("error"),
    })


@app.get("/download/{job_id}")
async def download_result(job_id: str):
    job = jobs.get(job_id)
    if not job or job["status"] != "done":
        raise HTTPException(status_code=404, detail="Result not ready")
    return Response(
        content=job["xlsx_bytes"],
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="balancete_extraido.xlsx"'},
    )


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
        logger.info("Processing %s \u2026", label)
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
            "Extracted %s in %.1fs \u2014 type=%s, periodo=%s, %d rows, %d warnings",
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
            logger.error("Skipping %s \u2014 structurally invalid extraction", label)
            errors.append({"pagina": label, "erro": "Extra\u00e7\u00e3o estruturalmente inv\u00e1lida"})

    return extractions, labels, errors


async def _process_job(job_id: str, pdf_inputs: list[tuple[bytes, str]]) -> None:
    """Background task that performs the heavy extraction work."""
    try:
        job = jobs[job_id]

        # -- Step 2: Split PDFs into pages ----------------------------------
        job["message"] = "Dividindo PDFs em p\u00e1ginas..."
        job["progress"] = 0.05

        pdf_groups: list[list[tuple[bytes, str]]] = []
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

        # -- Step 3: Extract ------------------------------------------------
        job["message"] = "Extraindo dados via IA..."
        job["progress"] = 0.10

        all_extractions: list[dict] = []
        all_labels: list[str] = []
        all_errors: list[dict] = []

        max_workers = min(len(pdf_groups), 4)
        loop = asyncio.get_running_loop()

        if max_workers <= 1:
            ext, lab, err = await loop.run_in_executor(None, _process_pdf_pages, pdf_groups[0])
            all_extractions.extend(ext)
            all_labels.extend(lab)
            all_errors.extend(err)
        else:
            logger.info("Processing %d PDFs in parallel (max_workers=%d)", len(pdf_groups), max_workers)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    loop.run_in_executor(executor, _process_pdf_pages, group)
                    for group in pdf_groups
                ]
                results = await asyncio.gather(*futures)
                for ext, lab, err in results:
                    all_extractions.extend(ext)
                    all_labels.extend(lab)
                    all_errors.extend(err)

        job["progress"] = 0.70

        if all_errors:
            logger.warning("Extraction completed with %d error(s)", len(all_errors))
            for e in all_errors:
                logger.warning("  %s: %s", e["pagina"], e["erro"])

        # -- Step 4: Consolidate --------------------------------------------
        job["message"] = "Consolidando e classificando..."
        job["progress"] = 0.75

        rows = consolidate(all_extractions, all_labels)
        logger.info("Consolidated: %d rows before dedup", len(rows))

        all_have_masks, missing_count = verificar_mascaras(rows)
        if all_have_masks:
            logger.info("All rows have masks \u2014 skipping generation")
        elif rows:
            ratio = missing_count / len(rows)
            if ratio >= MASK_GENERATION_THRESHOLD:
                logger.info(
                    "%d/%d rows missing masks (%.0f%%) \u2014 generating via AI",
                    missing_count, len(rows), ratio * 100,
                )
                rows = gerar_mascaras(rows)
            else:
                logger.info(
                    "Only %d/%d rows missing masks (%.0f%%) \u2014 skipping AI mask generation (threshold: %.0f%%)",
                    missing_count, len(rows), ratio * 100, MASK_GENERATION_THRESHOLD * 100,
                )

        rows = deduplicate(rows)
        logger.info("After dedup: %d rows", len(rows))

        job["progress"] = 0.85

        # Arithmetic validation
        arithmetic_errors = validar_aritmetica(rows)
        all_errors.extend(arithmetic_errors)
        logger.info("Arithmetic validation: %d inconsistencies found", len(arithmetic_errors))

        # Classify accounts against standard chart
        try:
            rows = classificar_contas(rows)
            logger.info("Classification complete")
        except Exception as exc:
            logger.error("Classification failed (continuing without): %s", exc)

        job["progress"] = 0.90

        # -- Step 5: Generate XLSX ------------------------------------------
        job["message"] = "Gerando Excel..."
        job["progress"] = 0.95

        xlsx_bytes = build_xlsx(rows, errors=all_errors)
        logger.info("XLSX generated: %d bytes", len(xlsx_bytes))

        job["xlsx_bytes"] = xlsx_bytes
        job["status"] = "done"
        job["progress"] = 1.0
        job["message"] = "Conclu\u00eddo"

    except Exception as e:
        logger.exception("Job %s failed", job_id)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["message"] = f"Erro: {e}"


@app.post("/extract")
async def extract_endpoint(
    files: list[UploadFile] = File(..., description="One or more PDF or ZIP files"),
) -> JSONResponse:
    """Main extraction endpoint.

    Accepts PDF files and/or ZIP archives containing PDFs.
    Returns a job_id immediately; use /status/{job_id} to poll progress
    and /download/{job_id} to fetch the result.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No PDF files uploaded.")

    # Cleanup old jobs on each request
    _cleanup_old_jobs()

    logger.info("Received %d file(s)", len(files))

    # -- Step 1: Read uploads, unzip if needed -----------------------------
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

    # -- Create job and launch background task -----------------------------
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "processing",
        "progress": 0.0,
        "message": "Iniciando extra\u00e7\u00e3o...",
        "xlsx_bytes": None,
        "error": None,
        "created_at": time.time(),
    }

    asyncio.create_task(_process_job(job_id, pdf_inputs))

    return JSONResponse(content={"job_id": job_id}, status_code=202)
