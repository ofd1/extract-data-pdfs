# extract-data-pdfs

Backend Python que recebe PDFs de balancetes contábeis, extrai dados via Gemini AI e devolve um Excel consolidado.

## Arquitetura
Lovable → n8n (ponte) → POST /extract (este backend) → XLSX response

### Fluxo interno

1. Recebe 1+ PDFs (ou ZIPs contendo PDFs) via `multipart/form-data`
2. Divide cada PDF em páginas individuais (PyMuPDF)
3. Processa PDFs em **paralelo** (páginas dentro de cada PDF são sequenciais, com contexto da página anterior)
4. Normaliza números, consolida e deduplica todas as linhas
5. Valida somas aritméticas (conta-pai = soma dos filhos via máscara contábil)
6. Classifica contas analíticas contra o Plano de Contas Padrão (De-Para via IA)
7. Gera XLSX com aba de dados e aba de relatório de erros

## Features

- **Contexto entre páginas**: Resumo da página N é passado para N+1 do mesmo PDF, melhorando extração de tabelas longas.
- **Processamento paralelo**: Múltiplos PDFs são processados simultaneamente (até 4 threads).
- **Threshold inteligente para máscaras**: Só gera máscaras via IA se ≥80% estiverem faltando (indica que o documento original não tem códigos). Se poucas faltam, assume erro pontual e ignora.
- **Validação aritmética**: Verifica que contas-pai = soma das contas-filhas usando a hierarquia de máscaras contábeis.
- **Classificação De-Para**: Contas analíticas são mapeadas ao Plano de Contas Padrão com sinal (+/-).
- **Relatório de erros**: Aba separada no Excel lista páginas com falha, extrações inválidas e inconsistências aritméticas.

## Deploy no Railway

1. Conecte este repo ao Railway
2. Adicione a variável de ambiente `GEMINI_API_KEY`
3. Opcional: `GEMINI_MODEL` (default: `gemini-2.0-flash`)
4. O deploy usa o Dockerfile automaticamente

## Variáveis de ambiente

| Variável | Obrigatória | Default | Descrição |
|---|---|---|---|
| `GEMINI_API_KEY` | Sim | — | Chave da API do Google Gemini |
| `GEMINI_MODEL` | Não | `gemini-2.0-flash` | Modelo Gemini a usar |
| `PORT` | Não | `8080` | Porta do servidor (Railway define automaticamente) |

## Endpoints

### `GET /health`
Health check. Retorna `{"status": "ok"}`.

### `POST /extract`
Recebe PDFs e retorna XLSX consolidado.

**Request:** `multipart/form-data` com campo `files` (1 ou mais PDFs ou ZIPs).

**Response:** arquivo `.xlsx` com duas abas:

**Aba "Balancete Consolidado"** — colunas:
- `Tipo` — BP ou DRE
- `Periodo` — período de referência (MM/YYYY)
- `Conta` — nome da conta
- `Mascara_Contabil` — código hierárquico (ex: 1.01.01)
- `Conta_Padronizada` — (reservado para uso futuro)
- `Sinal` — "+" ou "-" conforme natureza da conta
- `Classificacao_Padrao` — item do Plano de Contas Padrão (De-Para)
- `Ano_Anterior` — valor do ano anterior
- `Ano_Atual` — valor do ano atual
- `Pagina_Origem` — identificador PDF+página (ex: PDF1-P3)

**Aba "Relatório de Erros"** — colunas:
- `Página` — identificador da página com problema
- `Erro` — descrição do erro

## Desenvolvimento local
```bash
pip install -r requirements.txt
export GEMINI_API_KEY=sua-chave
uvicorn app.main:app --reload
```

Teste com curl:
```bash
curl -X POST http://localhost:8000/extract \
  -F "files=@balancete1.pdf" \
  -F "files=@balancete2.pdf" \
  -o resultado.xlsx
```

## Configuração no n8n

No workflow do n8n, use um nó **HTTP Request**:
- Method: `POST`
- URL: `https://seu-app.railway.app/extract`
- Body: `Form-Data/Multipart`
- Campo: `files` com o(s) PDF(s) recebido(s) do webhook
- Response: Binary (o XLSX volta como arquivo)
