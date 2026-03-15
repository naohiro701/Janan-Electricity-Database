# 全体アーキテクチャ

## 1. 収集
- `collectors/*` が公開ソースから raw bytes を取得します。
- HTTP 直取得を優先し、OCCTO 公開画面の CSV ダウンロードだけ Playwright を使います。
- raw は `data/raw/{source_name}/{biz_date}/` に保存し、同時に `sha256` と取得時刻を記録します。

## 2. 正規化
- `parsers/*` が raw を Polars DataFrame に変換します。
- 文字コード差異、列名ゆれ、JST 時刻化、wide-to-long 変換をここで完結させます。

## 3. 品質チェック
- `validators/*` が schema drift、主キー重複、not null、数値範囲、欠損日を検知します。
- 想定外の列変化は `source_snapshot.schema_signature` と `observed_columns` に残します。

## 4. 永続化
- `loaders/db.py` が `source_snapshot` を先に UPSERT し、その `snapshot_id` を各 fact に付けて書き込みます。
- fact は narrow table で保持し、時系列検索は TimescaleDB hypertable を前提にします。

## 5. 参照
- `apps/api/*` が FastAPI で `/spot`, `/intraday`, `/reserve-ratio`, `/intertie-flow`, `/trunk-flow`, `/generation-unit` を提供します。
- JSON 既定、`format=csv` で CSV export に切り替えます。

## 6. 運用
- `pipelines/ingest.py` が `now`, `daily`, `backfill` を統括します。
- GitHub Actions で lint/test/collect を分離し、失敗時は明示的にジョブを落とします。
