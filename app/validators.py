"""Validate data extracted from Gemini before consolidation."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

VALID_TYPES = {"BP", "DRE"}

REQUIRED_ROW_FIELDS = ("Conta", "Mascara_Contabil", "Ano_Anterior", "Ano_Atual")


def validar_extracao(extraction: dict, page_label: str) -> tuple[bool, list[str]]:
    """Validate a single page extraction result.

    Args:
        extraction: Dict returned by gemini_extractor.extract_page.
        page_label: Label like "PDF1-P3" for logging.

    Returns:
        (is_valid, list_of_warnings). is_valid is False only if the data is
        structurally broken (no rows list). Warnings are logged but don't
        block processing — the consolidator applies its own normalization.
    """
    warnings: list[str] = []

    # Check type
    tipo = extraction.get("type", "")
    if tipo not in VALID_TYPES:
        warnings.append(f"type '{tipo}' not in {VALID_TYPES} (will be normalized)")

    # Check periodo
    if not extraction.get("periodo"):
        warnings.append("periodo is missing or empty")

    # Check rows exist
    rows = extraction.get("rows")
    if not isinstance(rows, list):
        warnings.append("'rows' is not a list — extraction may have failed")
        for w in warnings:
            logger.warning("[%s] %s", page_label, w)
        return False, warnings

    if len(rows) == 0:
        warnings.append("0 rows extracted — page may be blank or non-tabular")

    # Check individual rows
    for i, row in enumerate(rows):
        for field in REQUIRED_ROW_FIELDS:
            if field not in row:
                warnings.append(f"row {i}: missing field '{field}'")

        # Check numeric fields
        for num_field in ("Ano_Anterior", "Ano_Atual"):
            val = row.get(num_field)
            if val is not None and not isinstance(val, (int, float)):
                try:
                    float(str(val).replace(",", "."))
                except (ValueError, TypeError):
                    warnings.append(f"row {i}: {num_field} = '{val}' is not numeric")

        # Check for code pollution in Conta
        conta = str(row.get("Conta", ""))
        if conta and conta[0].isdigit():
            warnings.append(f"row {i}: Conta starts with digit: '{conta[:50]}' (will be cleaned)")

    for w in warnings:
        logger.warning("[%s] %s", page_label, w)

    return True, warnings
