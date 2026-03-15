# Power Data Platform PoC

日本の公開電力データを対象にした、収集・正規化・監査・TimescaleDB格納・FastAPI参照を一体化した PoC です。

## 対象ソース
- OCCTO 地域間連系線潮流
- OCCTO 地内基幹送電線潮流
- OCCTO ユニット別発電実績
- JEPX スポット市場
- JEPX 時間前市場
- OCCTO 広域予備率

## 主な特徴
- raw 保存と `sha256` 監査
- `source_snapshot` による版管理メタデータ
- Polars による parser / validator
- psycopg による TimescaleDB UPSERT
- FastAPI 参照 API と CSV export
- `now` / `daily` / `backfill`
- schema drift / 主キー重複 / 欠損 / range 異常の検知

## エントリポイント
- 収集実行: `power-poc-ingest`
- API 起動: `power-poc-api`

## 主要ファイル
- [sources.yaml](sources.yaml)
- [schema.sql](schema.sql)
- [requirements.md](requirements.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/runbook.md](docs/runbook.md)
- [docs/assumptions.md](docs/assumptions.md)
