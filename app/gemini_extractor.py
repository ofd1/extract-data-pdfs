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
{{
  "periodo": "09/2023",
  "type": "BP",
  "ano_atual": "2024",
  "ano_anterior": "2023",
  "rows": [
    {{
      "Conta": "Caixa e Equivalentes",
      "Mascara_Contabil": "1.01.01",
      "Ano_Anterior": 1234.56,
      "Ano_Atual": 5678.90
    }}
  ]
}}

REGRAS:

1. PERIODO: Identifique o período de referência no cabeçalho. Formato "MM/YYYY" ou "YYYY". Se não encontrar, use "".

2. TYPE — APENAS dois valores:
   - "DRE" se o conteúdo for Demonstração do Resultado (palavras-chave: Receita, Despesa, Custo, Resultado Operacional, Lucro, Prejuízo).
   - "BP" para todo o resto (Balanço Patrimonial, Balancete, Ativo, Passivo, Patrimônio Líquido).

3. ANOS: "ano_atual" e "ano_anterior" são os rótulos das colunas de valores (ex: "2024", "2023"). Se só houver uma coluna, "ano_anterior" = "".

4. VALORES MONETÁRIOS — CRÍTICO:
   - Retorne decimais com PONTO como separador. Remova R$, pontos de milhar. Vírgula decimal vira ponto.
   - Exemplos: R$ 1.234,56 → 1234.56 | (1.234,56) → -1234.56 | 0,00 → 0
   - NÃO remova centavos. 1.234,56 NÃO é 123456.
   - DÉBITO e CRÉDITO: Se a tabela tiver indicadores D/C:
     • No Ativo e Despesas: D = valor positivo, C = valor negativo.
     • No Passivo e Receitas: C = valor positivo, D = valor negativo.
   - Se não houver D/C, mantenha o sinal numérico original (parênteses = negativo).

5. CONTA: Extraia APENAS o nome descritivo. Remova códigos numéricos e máscaras do início.

6. MASCARA_CONTABIL: Extraia o código hierárquico (ex: "1", "1.01", "1.01.01"). Se não houver, use "".

7. Extraia TODAS as linhas visíveis, sem omitir nenhuma. Se não houver valor, use 0.

8. Retorne APENAS o JSON válido, sem explicações, sem markdown, sem ```json.

{contexto_anterior}
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


def build_context_summary(extraction: dict) -> str:
    """Build a context string from a previous page extraction.

    Args:
        extraction: Dict returned by extract_page for the previous page.

    Returns:
        A formatted context string, or "" if the extraction has no rows.
    """
    rows = extraction.get("rows", [])
    if not rows:
        return ""

    first = rows[0]
    last = rows[-1]
    return (
        "CONTEXTO DA PÁGINA ANTERIOR (use para continuidade):\n"
        f"- Tipo: {extraction.get('type', '?')}\n"
        f"- Primeira linha: {first.get('Conta', '?')} ({first.get('Mascara_Contabil', '?')})\n"
        f"- Última linha: {last.get('Conta', '?')} ({last.get('Mascara_Contabil', '?')})"
    )


def extract_page(
    pdf_bytes: bytes,
    page_label: str = "",
    contexto_anterior: str = "",
) -> dict:
    """Send a single-page PDF to Gemini and return parsed JSON.

    Args:
        pdf_bytes: Raw bytes of a single-page PDF.
        page_label: Human-readable label for logging (e.g. "PDF1-Page3").
        contexto_anterior: Optional context from the previous page extraction,
            built via build_context_summary(). Injected into the prompt.

    Returns:
        Parsed dict with keys: type, periodo, ano_atual, ano_anterior, rows.
    """
    client = _get_client()
    model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

    prompt = EXTRACTION_PROMPT.format(contexto_anterior=contexto_anterior)

    pdf_part = types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")

    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model=model,
                contents=[pdf_part, prompt],
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
