# extract-data-pdfs

Backend Python que recebe PDFs de balancetes contábeis, extrai dados via Gemini AI e devolve um Excel consolidado.

## Arquitetura

```
Lovable → n8n (ponte) → POST /extract (este backend) → XLSX response
```

### Fluxo interno

1. Recebe 1+ PDFs via `multipart/form-data`
2. Divide cada PDF em páginas individuais (PyMuPDF)
3. Envia cada página ao Gemini API com prompt de extração
4. Normaliza números e consolida todas as linhas
5. Gera XLSX e retorna como download

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

**Request:** `multipart/form-data` com campo `files` (1 ou mais PDFs).

**Response:** arquivo `.xlsx` com colunas:
- `Tipo` — tipo do demonstrativo (BP, DRE, etc.)
- `Conta` — nome da conta
- `Mascara_Contabil` — código contábil (ex: 1.1.01.01)
- `Ano_Anterior` — valor do ano anterior
- `Ano_Atual` — valor do ano atual
- `Macro` — true se for linha de título/subtotal
- `Pagina_Origem` — identificador PDF+página (ex: PDF1-P3)

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
