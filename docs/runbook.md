# Runbook

## 1. Prerequisites
- Python `3.12`
- PostgreSQL + TimescaleDB
- Chromium for Playwright collectors

## 2. Environment variables

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/power_poc"
export RAW_DATA_ROOT="$(pwd)/data/raw"
export LOG_LEVEL="INFO"
```

## 3. Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
playwright install chromium
```

## 4. Initialize database

```bash
psql "$DATABASE_URL" -f schema.sql
```

## 5. Run collection

```bash
# now
power-poc-ingest --mode now --date 2026-03-15

# daily
power-poc-ingest --mode daily

# backfill
power-poc-ingest --mode backfill --from 2026-03-01 --to 2026-03-07

# parser / validator dry-run only
power-poc-ingest --mode now --date 2026-03-15 --skip-db
```

## 6. Start API

```bash
power-poc-api
```

- OpenAPI: `http://localhost:8000/docs`
- Home: `http://localhost:8000/`

## 7. Re-run / recovery
- raw は `data/raw/{source_name}/{biz_date}/` に残るため、失敗時は同日再実行でよいです。
- UPSERT key は `market_ts_jst` と dimension key に固定しているため、再実行時は `snapshot_id` と fact 値が更新されます。
- schema drift は validator で明示失敗するため、列変化時は `source_snapshot.observed_columns` を確認して parser を修正してください。

## 8. Operational checks
- ログに `source_name`, `biz_date`, `mode` が出ていること
- `source_snapshot` に raw path / sha256 / observed_columns が保存されていること
- fact row 数が再実行で増えすぎていないこと
- `/spot`, `/intraday`, `/reserve-ratio`, `/intertie-flow`, `/trunk-flow`, `/generation-unit` が参照できること

## 9. Failure handling
- JEPX/OCCTO 側の未公表日や画面変更は明示例外で止まります
- ブラウザ collector が失敗した場合は `docs/assumptions.md` の selector 前提を確認し、公開画面の DOM 差分を再点検してください
