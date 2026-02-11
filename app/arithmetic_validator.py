"""Validate that parent accounts equal the sum of their direct children."""

from __future__ import annotations

import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def _mask_level(mascara: str) -> int:
    """Return the hierarchy level of a mask. e.g. '1'=1, '1.01'=2, '1.01.01'=3."""
    return mascara.count(".") + 1


def _is_direct_child(parent: str, candidate: str) -> bool:
    """Check if candidate is exactly one level below parent.

    '1.01' is a direct child of '1'.
    '1.01.01' is NOT a direct child of '1' (two levels below).
    """
    if not candidate.startswith(parent + "."):
        return False
    return _mask_level(candidate) == _mask_level(parent) + 1


def validar_aritmetica(rows: list[dict]) -> list[dict]:
    """Validate parent-child arithmetic consistency across accounting masks.

    Groups rows by (Tipo, Periodo, Pagina_Origem) and within each group
    checks that every parent mask's values equal the sum of its direct
    children (tolerance: abs(diff) <= 1.0 for rounding).

    Args:
        rows: Consolidated and deduplicated row dicts.

    Returns:
        List of error dicts: {"pagina": ..., "erro": ...}.
    """
    errors: list[dict] = []

    # Group rows by independent balancete
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        mascara = row.get("Mascara_Contabil", "").strip()
        if not mascara:
            continue
        key = (row.get("Tipo", ""), row.get("Periodo", ""), row.get("Pagina_Origem", ""))
        groups[key].append(row)

    for group_key, group_rows in groups.items():
        pagina = group_key[2]  # Pagina_Origem

        # Build lookup by mask
        by_mask: dict[str, dict] = {}
        for r in group_rows:
            m = r["Mascara_Contabil"].strip()
            by_mask[m] = r

        all_masks = sorted(by_mask.keys())

        # Find parents: masks that have at least one direct child in the group
        for parent_mask in all_masks:
            children = [
                m for m in all_masks
                if _is_direct_child(parent_mask, m)
            ]
            if not children:
                continue

            parent_row = by_mask[parent_mask]
            parent_atual = parent_row.get("Ano_Atual", 0.0)
            parent_anterior = parent_row.get("Ano_Anterior", 0.0)

            soma_atual = sum(by_mask[c].get("Ano_Atual", 0.0) for c in children)
            soma_anterior = sum(by_mask[c].get("Ano_Anterior", 0.0) for c in children)

            diff_atual = abs(parent_atual - soma_atual)
            diff_anterior = abs(parent_anterior - soma_anterior)

            if diff_atual > 1.0:
                errors.append({
                    "pagina": pagina,
                    "erro": (
                        f"Soma inconsistente: {parent_mask} ({parent_row.get('Conta', '?')}) "
                        f"— Ano_Atual esperado: {soma_atual:.2f}, "
                        f"encontrado: {parent_atual:.2f}, "
                        f"diferença: {diff_atual:.2f}"
                    ),
                })

            if diff_anterior > 1.0:
                errors.append({
                    "pagina": pagina,
                    "erro": (
                        f"Soma inconsistente: {parent_mask} ({parent_row.get('Conta', '?')}) "
                        f"— Ano_Anterior esperado: {soma_anterior:.2f}, "
                        f"encontrado: {parent_anterior:.2f}, "
                        f"diferença: {diff_anterior:.2f}"
                    ),
                })

    logger.info("Arithmetic validation: %d inconsistencies found", len(errors))
    return errors
