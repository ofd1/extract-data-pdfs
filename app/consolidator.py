"""Normalize, deduplicate, and consolidate extracted rows into a flat table."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any


def normalize_number(raw: Any) -> float:
    """Convert a raw value to a float.

    Handles:
      - None, empty string, "-"  → 0
      - Already numeric types    → float(val)
      - Strings with R$, dots (thousands), commas (decimal)
        e.g. "R$ 1.234,56" → 1234.56
        e.g. "1234"        → 1234.0
        e.g. "(1.234,56)"  → -1234.56  (parentheses = negative)
    """
    if raw is None:
        return 0.0

    if isinstance(raw, (int, float)):
        return float(raw)

    s = str(raw).strip()
    if s in ("", "-", "–", "—"):
        return 0.0

    # Detect negative via parentheses: (1.234,56)
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1].strip()

    # Remove currency symbol and whitespace
    s = re.sub(r"[R$\s]", "", s)

    # Determine decimal separator heuristic:
    # If the string has both dots and commas, the last one is decimal.
    # Brazilian format: 1.234,56  (dot=thousands, comma=decimal)
    # US format:        1,234.56  (comma=thousands, dot=decimal)
    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            # Brazilian: dots are thousands, comma is decimal
            s = s.replace(".", "").replace(",", ".")
        else:
            # US: commas are thousands, dot is decimal
            s = s.replace(",", "")
    elif "," in s:
        # Only commas — treat as decimal separator (Brazilian)
        s = s.replace(",", ".")
    # If only dots, leave as-is (could be thousands or decimal; if Gemini
    # follows instructions it will already be a plain integer).

    try:
        val = float(s)
    except ValueError:
        return 0.0

    return -val if negative else val


_DRE_KEYWORDS = ("receita", "despesa", "custo", "resultado", "dre", "demonstra", "lucro", "prejuízo")


def _normalize_tipo(tipo: str) -> str:
    """Map any variant of the type field to 'BP' or 'DRE'."""
    t = tipo.strip().upper()
    if t == "DRE":
        return "DRE"
    low = tipo.lower()
    if any(kw in low for kw in _DRE_KEYWORDS):
        return "DRE"
    return "BP"


def _clean_conta(conta: str) -> str:
    """Remove leading numeric codes and accounting masks from account names.

    Examples:
        "1002518 1.01.02.001.001 CLAUDIO ROSSI" → "CLAUDIO ROSSI"
        "310749 1.01.02.001 Companhia Uci"      → "Companhia Uci"
        "Caixa e Equivalentes"                   → "Caixa e Equivalentes"
    """
    # Pattern: optional digits, optional mask (digits+dots), then the name
    m = re.match(r'^\d+\s+[\d.]+\s+(.+)$', conta)
    if m:
        return m.group(1).strip()
    # Also handle: just a mask prefix like "1.01.01 Caixa"
    m2 = re.match(r'^[\d.]+\s+(.+)$', conta)
    if m2 and not conta[0].isalpha():
        return m2.group(1).strip()
    return conta


def consolidate(
    extractions: list[dict],
    page_labels: list[str],
) -> list[dict]:
    """Merge extracted page data into a single flat row list.

    Args:
        extractions: List of dicts returned by gemini_extractor.extract_page,
                     one per page, IN ORDER (PDF1-page1, PDF1-page2, ..., PDF2-page1, ...).
        page_labels: Parallel list of labels like "PDF1-P1".

    Returns:
        List of row dicts ready for Excel:
          Tipo, Periodo, Conta, Mascara_Contabil, Conta_Padronizada,
          Sinal, Classificacao_Padrao, Ano_Anterior, Ano_Atual, Pagina_Origem
    """
    rows: list[dict] = []

    for extraction, label in zip(extractions, page_labels):
        tipo = _normalize_tipo(extraction.get("type", ""))
        periodo = str(extraction.get("periodo", "")).strip()
        ano_atual = extraction.get("ano_atual", "")
        ano_anterior = extraction.get("ano_anterior", "")

        for row in extraction.get("rows", []):
            rows.append({
                "Tipo": tipo,
                "Periodo": periodo,
                "Conta": _clean_conta(str(row.get("Conta", "")).strip()),
                "Mascara_Contabil": str(row.get("Mascara_Contabil", "")).strip(),
                "Conta_Padronizada": "",
                "Sinal": "",
                "Classificacao_Padrao": "",
                "Ano_Anterior": normalize_number(row.get("Ano_Anterior", 0)),
                "Ano_Atual": normalize_number(row.get("Ano_Atual", 0)),
                "Pagina_Origem": label,
                "_ano_atual_label": ano_atual,
                "_ano_anterior_label": ano_anterior,
            })

    return rows


def deduplicate(rows: list[dict]) -> list[dict]:
    """Group rows by (Mascara_Contabil, Conta, Tipo, Pagina_Origem) and sum numeric values.

    Duplicates can happen when the same row spans two visual blocks on the
    same page, or when Gemini repeats a row.  We only merge exact key matches
    within the SAME page to avoid collapsing legitimately repeated accounts
    across different pages/years.
    """
    key_fn = lambda r: (r["Tipo"], r["Mascara_Contabil"], r["Conta"], r["Pagina_Origem"])

    groups: dict[tuple, dict] = {}
    order: list[tuple] = []

    for row in rows:
        k = key_fn(row)
        if k not in groups:
            groups[k] = dict(row)  # copy
            order.append(k)
        else:
            groups[k]["Ano_Anterior"] += row["Ano_Anterior"]
            groups[k]["Ano_Atual"] += row["Ano_Atual"]

    return [groups[k] for k in order]
