"""Generate an XLSX workbook from consolidated rows."""

from __future__ import annotations

import io

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, numbers


HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
HEADER_FILL = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
MACRO_FILL = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
MACRO_FONT = Font(bold=True)

COLUMNS = [
    ("Tipo", 8),
    ("Periodo", 16),
    ("Conta", 45),
    ("Mascara_Contabil", 20),
    ("Conta_Padronizada", 45),
    ("Ano_Anterior", 18),
    ("Ano_Atual", 18),
    ("Macro", 8),
    ("Pagina_Origem", 16),
]


def build_xlsx(rows: list[dict]) -> bytes:
    """Create an XLSX file in memory and return its bytes.

    Args:
        rows: Consolidated row dicts with keys matching COLUMNS.

    Returns:
        Raw bytes of the .xlsx file.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Balancete Consolidado"

    # --- Header row ---
    for col_idx, (col_name, width) in enumerate(COLUMNS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center")
        ws.column_dimensions[cell.column_letter].width = width

    # --- Data rows ---
    for row_idx, row in enumerate(rows, start=2):
        is_macro = row.get("Macro", False)

        for col_idx, (col_name, _) in enumerate(COLUMNS, start=1):
            value = row.get(col_name, "")
            cell = ws.cell(row=row_idx, column=col_idx, value=value)

            if is_macro:
                cell.font = MACRO_FONT
                cell.fill = MACRO_FILL

            # Number formatting for monetary columns
            if col_name in ("Ano_Anterior", "Ano_Atual"):
                cell.number_format = '#,##0.00'
                cell.alignment = Alignment(horizontal="right")

    # Auto-filter
    if rows:
        ws.auto_filter.ref = f"A1:I{len(rows) + 1}"

    # Freeze header row
    ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()
