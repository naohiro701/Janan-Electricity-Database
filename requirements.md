# requirements.md

## Runtime

| Package | Version band | Purpose |
| --- | --- | --- |
| `fastapi` | `>=0.115,<1` | 参照 API |
| `uvicorn` | `>=0.34,<1` | API サーバ起動 |
| `httpx` | `>=0.28,<1` | HTTP collector |
| `playwright` | `>=1.52,<2` | browser collector |
| `polars` | `>=1.18,<2` | parser / validator |
| `psycopg[binary]` | `>=3.2,<4` | PostgreSQL / TimescaleDB 接続 |
| `pyyaml` | `>=6.0,<7` | `sources.yaml` 読み込み |
| `python-dateutil` | `>=2.9,<3` | date range / backfill 補助 |
| `pydantic` | `>=2.10,<3` | FastAPI schema |
| `python-multipart` | `>=0.0.20,<1` | FastAPI export 補助 |

## Dev / Test

| Package | Version band | Purpose |
| --- | --- | --- |
| `pytest` | `>=8.3,<9` | unit / integration tests |
| `ruff` | `>=0.9,<1` | lint / import / style gate |

## Python
- Python `3.12`

## Installation example

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
playwright install chromium
```
