"""Split multi-page PDFs into single-page PDF byte buffers."""

from __future__ import annotations

import fitz  # PyMuPDF


def split_pdf_to_pages(pdf_bytes: bytes) -> list[bytes]:
    """Return a list where each element is a single-page PDF as bytes.

    Pages are returned in their original order.
    """
    src = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages: list[bytes] = []
    for page_num in range(len(src)):
        dst = fitz.open()
        dst.insert_pdf(src, from_page=page_num, to_page=page_num)
        pages.append(dst.tobytes(deflate=True))
        dst.close()
    src.close()
    return pages
