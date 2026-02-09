"""Send single-page PDFs to Gemini and parse the structured JSON response."""

from __future__ import annotations

import json
import logging
import os
import re
import time

from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

EXTRACTION_PROMPT = """\
Extraia TODAS as linhas do balancete contábil desta página.
Retorne um JSON no formato:
{
  "type": "BP",
  "ano_atual": "2024",
  "ano_anterior": "2023",
  "rows": [
    {
      "Conta": "nome da conta",
      "Mascara_Contabil": "1.1.01.01",
      "Ano_Anterior": 1000,
      "Ano_Atual": 1500,
      "Macro": false
    }
  ]
}
REGRAS:
- "type" deve ser inferido do conteúdo: "BP" para Balanço Patrimonial, "DRE" para Demonstração do Resultado, ou o tipo que melhor se encaixar.
- "ano_atual" e "ano_anterior" devem ser os anos exibidos nas colunas do balancete (ex: "2024", "2023"). Se não houver ano anterior, use "".
- Extraia TODAS as linhas visíveis, sem omitir nenhuma.
- Use apenas números (sem R$, pontos de milhar ou vírgulas decimais — converta 1.234,56 para 123456).
- Macro = true se a linha for título, subtotal ou total de grupo.
- Se não houver valor, use 0.
- Retorne APENAS o JSON válido, sem explicações, sem markdown, sem ```json.
"""

# Retry config
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, doubled each retry


def _get_client() -> genai.Client:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY environment variable is not set")
    return genai.Client(api_key=api_key)


def _parse_json_response(text: str) -> dict:
    """Extract JSON from Gemini's response, handling markdown fences."""
    cleaned = text.strip()
    # Remove markdown code fences if present
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
    if match:
        cleaned = match.group(1).strip()
    return json.loads(cleaned)


def extract_page(pdf_bytes: bytes, page_label: str = "") -> dict:
    """Send a single-page PDF to Gemini and return parsed JSON.

    Args:
        pdf_bytes: Raw bytes of a single-page PDF.
        page_label: Human-readable label for logging (e.g. "PDF1-Page3").

    Returns:
        Parsed dict with keys: type, ano_atual, ano_anterior, rows.
    """
    client = _get_client()
    model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

    pdf_part = types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")

    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model=model,
                contents=[pdf_part, EXTRACTION_PROMPT],
            )
            result = _parse_json_response(response.text)
            logger.info("Extracted %s: %d rows", page_label, len(result.get("rows", [])))
            return result
        except json.JSONDecodeError as exc:
            logger.warning(
                "JSON parse error on %s (attempt %d): %s — raw: %s",
                page_label, attempt + 1, exc,
                response.text[:500] if response else "no response",
            )
            last_err = exc
        except Exception as exc:
            logger.warning("Gemini error on %s (attempt %d): %s", page_label, attempt + 1, exc)
            last_err = exc

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_BACKOFF * (2 ** attempt))

    raise RuntimeError(
        f"Failed to extract data from {page_label} after {MAX_RETRIES} attempts: {last_err}"
    )
