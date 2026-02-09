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
  "periodo": "09/2023",
  "type": "BP",
  "ano_atual": "2024",
  "ano_anterior": "2023",
  "rows": [
    {
      "Conta": "Caixa e Equivalentes",
      "Mascara_Contabil": "1.01.01",
      "Ano_Anterior": 1234.56,
      "Ano_Atual": 5678.90,
      "Macro": false
    }
  ]
}

REGRAS:

1. PERIODO:
   - Identifique o período de referência no cabeçalho do documento.
   - Formatos aceitos: "MM/YYYY" (mensal), "YYYY" (anual), "MM/YYYY a MM/YYYY" (intervalo).
   - Se não encontrar, use "".

2. TYPE — use APENAS estes valores:
   - "DRE" se o conteúdo for Demonstração do Resultado (palavras-chave: Receita, Despesa, Custo, Resultado Operacional, Lucro, Prejuízo).
   - "BP" para todo o resto (Balanço Patrimonial, Balancete, Ativo, Passivo, Patrimônio Líquido).

3. ANOS:
   - "ano_atual" e "ano_anterior" são os anos exibidos nas colunas (ex: "2024", "2023").
   - Se não houver ano anterior, use "".

4. VALORES MONETÁRIOS — CRÍTICO:
   - Retorne números decimais usando PONTO como separador decimal.
   - Remova R$, pontos de milhar, e converta vírgula decimal para ponto.
   - Exemplos:
     R$ 1.234,56  → 1234.56
     R$ 10.500,00 → 10500.00
     415.883,97   → 415883.97
     (1.234,56)   → -1234.56
     0,00         → 0
   - NÃO remova os centavos. 1.234,56 NÃO é 123456.

5. CONTA — extraia APENAS o nome descritivo:
   - Remova códigos numéricos e máscaras contábeis do início.
   - Exemplos:
     Errado: "1002518 1.01.02.001 CLAUDIO ROSSI" → Certo: "CLAUDIO ROSSI"
     Errado: "310749 1.01.02.001 Companhia Uci"  → Certo: "Companhia Uci"
     Correto: "Caixa e Equivalentes de Caixa"

6. MASCARA_CONTABIL:
   - Extraia o código hierárquico (ex: "1", "1.01", "1.01.01", "1.01.01.001").
   - Se não houver máscara visível, use "".

7. MACRO:
   - true se a linha for título de grupo, subtotal ou total.
   - false para contas analíticas (folha/detalhe).

8. GERAL:
   - Extraia TODAS as linhas visíveis, sem omitir nenhuma.
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
            logger.info(
                "Extracted %s: type=%s, periodo=%s, %d rows",
                page_label,
                result.get("type", "?"),
                result.get("periodo", "?"),
                len(result.get("rows", [])),
            )
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
