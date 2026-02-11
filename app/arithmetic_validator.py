"""Validate that parent accounts equal the sum of their direct children using Mascara_Contabil hierarchy."""

from __future__ import annotations

import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def _get_level(mascara: str) -> int:
    """Return the hierarchical level of a mask.

    Examples:
        "1"       → 1
        "1.01"    → 2
        "1.01.01" → 3
    """
    return mascara.count(".") + 1


def _is_direct_child(parent_mask: str, child_mask: str) -> bool:
    """Return True if *child_mask* is a direct child of *parent_mask*.

    A mask Y is a direct child of X when:
      (a) Y starts with X + "."
      (b) Y has exactly one more level than X.
    """
    return (
        child_mask.startswith(parent_mask + ".")
        and _get_level(child_mask) == _get_level(parent_mask) + 1
    )


def validar_aritmetica(rows: list[dict]) -> list[dict]:
    """Validate arithmetic consistency between parent and child accounts.

    Groups rows by (Tipo, Periodo, Pagina_Origem) and, for every parent mask
    that has direct children in the same group, checks that the parent's
    Ano_Atual and Ano_Anterior equal the sum of its direct children (within a
    tolerance of 1.0 for rounding).

    Returns a list of error dicts (may be empty).
    """
    errors: list[dict] = []

    # 1. Group rows by (Tipo, Periodo, Pagina_Origem)
    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in rows:
        key = (
            str(row.get("Tipo", "")).strip(),
            str(row.get("Periodo", "")).strip(),
            str(row.get("Pagina_Origem", "")).strip(),
        )
        groups[key].append(row)

    # 2. Validate each group independently
    for (tipo, periodo, pagina), group_rows in groups.items():
        # a. Filter rows that have a non-empty Mascara_Contabil
        filtered = [
            r for r in group_rows
            if str(r.get("Mascara_Contabil", "")).strip()
        ]

        # b. Build {mascara: row} for quick lookup (first occurrence wins)
        mask_map: dict[str, dict] = {}
        for r in filtered:
            m = str(r.get("Mascara_Contabil", "")).strip()
            if m not in mask_map:
                mask_map[m] = r

        # c. Collect all masks
        all_masks = set(mask_map.keys())

        # d/e. For each mask, check if it has direct children
        n_parents = 0
        n_errors = 0

        for parent_mask in all_masks:
            children_masks = [
                cm for cm in all_masks if _is_direct_child(parent_mask, cm)
            ]
            if not children_masks:
                continue

            n_parents += 1

            parent_row = mask_map[parent_mask]
            conta_pai = str(parent_row.get("Conta", "")).strip()

            valor_pai_atual = float(parent_row.get("Ano_Atual", 0) or 0)
            valor_pai_ant = float(parent_row.get("Ano_Anterior", 0) or 0)

            soma_filhos_atual = sum(
                float(mask_map[cm].get("Ano_Atual", 0) or 0)
                for cm in children_masks
            )
            soma_filhos_ant = sum(
                float(mask_map[cm].get("Ano_Anterior", 0) or 0)
                for cm in children_masks
            )

            diff_atual = soma_filhos_atual - valor_pai_atual
            diff_ant = soma_filhos_ant - valor_pai_ant

            if abs(diff_atual) > 1.0 or abs(diff_ant) > 1.0:
                n_errors += 1
                errors.append({
                    "pagina": pagina,
                    "erro": (
                        f"Soma inconsistente: {parent_mask} ({conta_pai}) — "
                        f"Ano_Atual: esperado {soma_filhos_atual:.2f}, "
                        f"encontrado {valor_pai_atual:.2f} (diff {diff_atual:.2f}) | "
                        f"Ano_Anterior: esperado {soma_filhos_ant:.2f}, "
                        f"encontrado {valor_pai_ant:.2f} (diff {diff_ant:.2f})"
                    ),
                })

        logger.info(
            "Arithmetic validation for group (%s, %s, %s): %d parents checked, %d inconsistencies",
            tipo, periodo, pagina, n_parents, n_errors,
        )

    return errors
