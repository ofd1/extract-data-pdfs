"""Verify and auto-generate accounting masks (Mascara_Contabil) when missing."""

from __future__ import annotations

import json
import logging
import os
import re
import time

from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF = 2

MASK_GENERATION_PROMPT = """\
Você receberá uma lista de contas contábeis extraídas de um balancete.
Algumas contas não possuem máscara contábil (código hierárquico).

Sua tarefa é GERAR as máscaras contábeis faltantes no padrão brasileiro.

Regras de hierarquia:
- Primeiro nível: 1, 2, 3, 4...
- Segundo nível: 1.01, 1.02, 2.01...
- Terceiro nível: 1.01.01, 1.01.02...
- Quarto nível: 1.01.01.001, 1.01.01.002...

Analise a estrutura hierárquica das contas (títulos, subtotais, contas analíticas)
e as máscaras já existentes para inferir as máscaras faltantes.

Entrada (JSON):
{entries}

Retorne um JSON com APENAS as contas que receberam máscara nova:
[
  {{"index": 0, "Conta": "ATIVO", "mascara": "1"}},
  {{"index": 3, "Conta": "Caixa", "mascara": "1.01.01.001"}}
]

Retorne APENAS o JSON válido, sem explicações.
"""


def verificar_mascaras(rows: list[dict]) -> tuple[bool, int]:
    """Check how many rows are missing accounting masks.

    Only checks non-Macro rows, since totals/subtotals sometimes
    legitimately lack masks.

    Returns:
        (all_have_masks, count_missing)
    """
    missing = 0
    for row in rows:
        mascara = row.get("Mascara_Contabil", "").strip()
        if not mascara:
            missing += 1

    logger.info("Mask check: %d/%d rows missing masks", missing, len(rows))
    return missing == 0, missing


def gerar_mascaras(rows: list[dict]) -> list[dict]:
    """Use Gemini to generate accounting masks for rows that lack them.

    Sends the list of accounts (with existing masks where available) to
    Gemini and fills in the blanks. Processes in chunks to stay within
    token limits.

    Args:
        rows: Consolidated row dicts. Modified in-place AND returned.

    Returns:
        The same list with Mascara_Contabil filled where possible.
    """
    # Build entries for rows missing masks, along with context from rows that have them
    entries = []
    for i, row in enumerate(rows):
        entries.append({
            "index": i,
            "Conta": row.get("Conta", ""),
            "Mascara_Contabil": row.get("Mascara_Contabil", ""),
            "Macro": row.get("Macro", False),
            "Tipo": row.get("Tipo", ""),
        })

    # Process in chunks of 200 rows to avoid token limits
    chunk_size = 200
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        logger.error("GEMINI_API_KEY not set — cannot generate masks")
        return rows

    client = genai.Client(api_key=api_key)
    model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

    for chunk_start in range(0, len(entries), chunk_size):
        chunk = entries[chunk_start:chunk_start + chunk_size]

        # Skip chunk if all entries already have masks
        if all(e["Mascara_Contabil"] for e in chunk):
            continue

        prompt = MASK_GENERATION_PROMPT.format(entries=json.dumps(chunk, ensure_ascii=False))

        last_err: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=[prompt],
                )
                text = response.text.strip()
                # Remove markdown fences
                match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
                if match:
                    text = match.group(1).strip()

                updates = json.loads(text)
                applied = 0
                for upd in updates:
                    idx = upd.get("index")
                    mascara = upd.get("mascara", "").strip()
                    if idx is not None and mascara and 0 <= idx < len(rows):
                        if not rows[idx].get("Mascara_Contabil", "").strip():
                            rows[idx]["Mascara_Contabil"] = mascara
                            applied += 1

                logger.info(
                    "Mask generation chunk %d-%d: %d masks applied",
                    chunk_start, chunk_start + len(chunk), applied,
                )
                break
            except (json.JSONDecodeError, Exception) as exc:
                logger.warning(
                    "Mask generation attempt %d failed: %s", attempt + 1, exc
                )
                last_err = exc
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))

        if last_err and attempt == MAX_RETRIES - 1:
            logger.error(
                "Mask generation failed for chunk %d-%d after %d attempts: %s",
                chunk_start, chunk_start + len(chunk), MAX_RETRIES, last_err,
            )

    return rows
