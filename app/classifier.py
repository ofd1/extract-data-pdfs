"""Classify analytical accounts against a standard Chart of Accounts using Gemini."""

from __future__ import annotations

import json
import logging
import os
import re
import time

from google import genai

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF = 2

PLANO_DE_CONTAS = [
    "(+) Receita 1", "(+) Receita 2", "(+) Outras Receitas",
    "(-) Deduções da Receita", "(-) Impostos s/ Receita", "(-) ISS", "(-) PIS", "(-) COFINS",
    "(-) Descontos e Devoluções",
    "(-) Custo do Serviço Prestado", "(-) Equipe", "(-) Servidor/Cloud", "(-) Software", "(-) Ocupação", "(-) D&A",
    "(-) Despesas Comerciais", "(-) Equipe de Originação", "(-) Equipe de CS", "(-) Parceirias Comerciais", "(-) Viagens e Estadias",
    "(-) Despesas de Marketing", "(-) Equipe de Marketing", "(-) Ações de Marketing", "(-) Eventos", "(-) Outras Despesas de Marketing",
    "(-) Despesas Gerais e Administrativas", "(-) Equipe Administrativa e RH", "(-) Serviços de Terceiros", "(-) Tributárias", "(-) Demais G&A", "(-) C-Level",
    "(+) D&A", "(+) Resultado Financeiro", "(+) Receitas Financeiras", "(-) Despesas Financeiras",
    "(+) Resultado não Operacional", "(+) Receitas não Operacionais", "(-) Despesas não Operacionais",
    "(-) IRPJ e CSLL", "(-) IRPJ", "(-) CSLL",
    "(+) Caixa e Equivalentes de Caixa", "(+) Clientes", "(+) Despesas Pagas Antecipadamente", "(+) Outros Créditos",
    "(+) Realizavel a Longo Prazo",
    "(+) Imobilizado", "(+) Bens em operação", "(-) Depreciação",
    "(+) Intangivel", "(+) Softwares, Projetos", "(-) Depreciação",
    "(+) Emprestimos e Financiamentos Curto Prazo", "(+) Dividendos a Distribuir", "(+) Fornecedores",
    "(+) Obrigações Trabalhistas e Prividenciárias", "(+) Obrigações Tributárias", "(+) Outras Obrigações",
    "(+) Emprestimos e Financiamentos Longo Prazo", "(+) Mútuos Conversiveis",
    "(+) Capital Social", "(+) Reserva de Lucros", "(+) Lucros e Prejuízos Acumulados", "(+) Resultado do Exercício",
]

CLASSIFICATION_PROMPT = """\
Você receberá uma lista de contas contábeis e deve classificar cada uma de acordo com o Plano de Contas Padrão abaixo.

Plano de Contas Padrão:
{plano_de_contas}

Para cada conta, retorne:
- "Classificacao_Padrao": o item do plano que melhor se encaixa (copie exatamente, incluindo o sinal entre parênteses).
- "Sinal": "+" ou "-" extraído da classificação escolhida.

Se uma conta for claramente um título/subtotal/total (como "ATIVO TOTAL", "PASSIVO TOTAL", "TOTAL DO PATRIMÔNIO LÍQUIDO", "ATIVO", "PASSIVO"), classifique como Classificacao_Padrao = "" e Sinal = "".

Entrada (JSON):
{entries}

Retorne um JSON array:
[
  {{"index": 0, "Classificacao_Padrao": "(+) Caixa e Equivalentes de Caixa", "Sinal": "+"}},
  {{"index": 1, "Classificacao_Padrao": "(-) Fornecedores", "Sinal": "-"}}
]

Retorne APENAS o JSON válido, sem explicações, sem markdown, sem ```json.
"""


def _get_client() -> genai.Client:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY environment variable is not set")
    return genai.Client(api_key=api_key)


def _parse_json_response(text: str) -> list | dict:
    """Extract JSON from Gemini's response, handling markdown fences."""
    cleaned = text.strip()
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
    if match:
        cleaned = match.group(1).strip()
    return json.loads(cleaned)


def identificar_contas_analiticas(rows: list[dict]) -> list[int]:
    """Return indices of rows that are analytical (leaf-level) accounts.

    A row is analytical if:
      (a) Its Mascara_Contabil is NOT a prefix of any other mask in the set, OR
      (b) It has no Mascara_Contabil (empty) — considered analytical by default.
    """
    masks: set[str] = {
        r["Mascara_Contabil"].strip()
        for r in rows
        if r.get("Mascara_Contabil", "").strip()
    }

    indices: list[int] = []
    for i, row in enumerate(rows):
        mascara = str(row.get("Mascara_Contabil", "")).strip()
        if not mascara:
            # No mask → analytical by default
            indices.append(i)
            continue
        # Check if any other mask starts with mascara + "."
        is_parent = any(m.startswith(mascara + ".") for m in masks if m != mascara)
        if not is_parent:
            indices.append(i)

    return indices


def classificar_contas(rows: list[dict]) -> list[dict]:
    """Classify analytical accounts against the standard chart of accounts via Gemini.

    Modifies rows in-place by adding 'Classificacao_Padrao' and 'Sinal' fields
    to each analytical account. Returns the modified rows list.
    """
    indices = identificar_contas_analiticas(rows)
    if not indices:
        logger.info("No analytical accounts found — skipping classification")
        return rows

    logger.info("Found %d analytical accounts to classify", len(indices))

    # Build entries for Gemini
    all_entries = [
        {
            "index": i,
            "Conta": str(rows[i].get("Conta", "")),
            "Tipo": str(rows[i].get("Tipo", "")),
            "Mascara_Contabil": str(rows[i].get("Mascara_Contabil", "")),
        }
        for i in indices
    ]

    client = _get_client()
    model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
    plano_text = "\n".join(PLANO_DE_CONTAS)

    # Process in chunks of 150
    chunk_size = 150
    for start in range(0, len(all_entries), chunk_size):
        end = min(start + chunk_size, len(all_entries))
        entries_chunk = all_entries[start:end]

        prompt = CLASSIFICATION_PROMPT.format(
            plano_de_contas=plano_text,
            entries=json.dumps(entries_chunk, ensure_ascii=False),
        )

        last_err: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=[prompt],
                )
                updates = _parse_json_response(response.text)

                applied = 0
                for item in updates:
                    idx = item.get("index")
                    if idx is not None and 0 <= idx < len(rows):
                        rows[idx]["Classificacao_Padrao"] = item.get("Classificacao_Padrao", "")
                        rows[idx]["Sinal"] = item.get("Sinal", "")
                        applied += 1

                logger.info(
                    "Classification chunk %d-%d: %d accounts classified",
                    start, end, applied,
                )
                break  # success — move to next chunk

            except json.JSONDecodeError as exc:
                logger.warning(
                    "JSON parse error on classification chunk %d-%d (attempt %d): %s — raw: %s",
                    start, end, attempt + 1, exc,
                    response.text[:500] if response else "no response",
                )
                last_err = exc
            except Exception as exc:
                logger.warning(
                    "Gemini error on classification chunk %d-%d (attempt %d): %s",
                    start, end, attempt + 1, exc,
                )
                last_err = exc

            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF * (2 ** attempt))
        else:
            raise RuntimeError(
                f"Failed to classify chunk {start}-{end} after {MAX_RETRIES} attempts: {last_err}"
            )

    return rows
