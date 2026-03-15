# Power Data Platform PoC

日本の公開電力データを対象にした、収集・正規化・監査・TimescaleDB格納・FastAPI参照を一体化した PoC です。

## すぐ使う

### 1. 設定ファイルを作る

```bash
cp .env.example .env
```

公開前提で使うなら、まず `.env` の `POSTGRES_PASSWORD` を変更してください。

### 2. DB だけ起動する

```bash
docker compose up -d db
docker compose ps
```

- DB は既定で `127.0.0.1:${DB_PORT}` にだけ bind されます
- そのままでは外部ネットワークに DB を公開しません

DB だけ起動した場合は、最初に schema を適用します。

```bash
docker compose run --rm api python -m pipelines.init_db
```

### 3. DB と API をまとめて起動する

```bash
docker compose up --build -d
docker compose ps
```

- API: `http://localhost:8000`
- OpenAPI: `http://localhost:8000/docs`
- Healthcheck: `http://localhost:8000/healthz`
- DB: `127.0.0.1:5432`

`docker compose up` 時に API コンテナが `schema.sql` を適用してから起動します。

### 4. 起動確認

```bash
curl http://localhost:8000/healthz
docker compose logs api --tail=100
docker compose logs db --tail=100
```

### 5. データを投入

DB/API を起動したあと、ローカル Python 環境または GitHub Actions から収集を流します。

```bash
# 例: ローカル実行
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
playwright install chromium

export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/power_poc"
export RAW_DATA_ROOT="$(pwd)/data/raw"

power-poc-ingest --mode now --date 2026-03-15
```

ブラウザ操作が必要な OCCTO 系収集は Playwright の browser binary が必要です。ローカル実行時は `playwright install chromium` を実行してください。API 用 Docker イメージは公開・参照用途を優先しているため、collect はローカル Python か GitHub Actions 実行を推奨します。

## GitHub に公開する方法

### 1. リポジトリを GitHub に push

```bash
git init
git add .
git commit -m "Initial PoC"
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

### 2. GitHub Actions を有効化

このリポジトリには以下の workflow を入れています。

- `.github/workflows/test.yml`
- `.github/workflows/collect.yml`
- `.github/workflows/docker-publish.yml`

`main` へ push すると、`docker-publish.yml` が API コンテナを GHCR に publish します。

公開イメージ名:

```text
ghcr.io/<github-user>/<repo>:latest
```

### 3. GitHub Secrets を設定

最低限、collect workflow で外部 DB を使うなら次を設定してください。

- `DATABASE_URL`

設定しない場合、workflow は job 内の TimescaleDB service を使います。

### 4. GHCR package を public にする

初回 push 後に GitHub の `Packages` から `ghcr.io/<github-user>/<repo>` を開き、Package settings で visibility を `Public` に変更してください。これを行わないと、外部ホストから `docker pull ghcr.io/<github-user>/<repo>:latest` できない場合があります。

## 外部公開する方法

GitHub Pages では FastAPI + PostgreSQL/TimescaleDB は動きません。GitHub はコード保管と CI/CD、GHCR は Docker image 配布に使い、実際の公開先は別ホストにします。

### 方法 A: Docker 対応サーバにそのまま載せる

VPS / VM / 社内サーバなど Docker が使えるホストで:

```bash
git clone <YOUR_GITHUB_REPO_URL>
cd <repo>
cp .env.example .env
vi .env
docker compose up -d --build
```

最低限、次を実施してください。

- `.env` の `POSTGRES_PASSWORD` を変更する
- `DB_BIND_HOST=127.0.0.1` のままにして DB を外へ出さない
- `API_PORT` の前段に Nginx / Caddy / Cloudflare Tunnel などの reverse proxy を置く
- TLS 終端は reverse proxy 側で行う

公開確認の例:

```bash
curl http://127.0.0.1:8000/healthz
```

### 方法 B: GHCR イメージを PaaS に載せる

`docker-publish.yml` が出力する `ghcr.io/<github-user>/<repo>:latest` を、Docker image を受け取れる PaaS にそのまま渡してください。必要な環境変数は次です。

- `DATABASE_URL`
- `RAW_DATA_ROOT`
- `LOG_LEVEL`

DB は同じ PaaS の managed PostgreSQL/TimescaleDB か、外部の PostgreSQL/TimescaleDB を指定します。

## 運用メモ

- DB を止める: `docker compose stop db`
- 全部止める: `docker compose down`
- DB データも消す: `docker compose down -v`
- API を再起動: `docker compose restart api`
- イメージ更新後に再作成: `docker compose up -d --build`

## 構成

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
- DB 初期化: `python -m pipelines.init_db`

## 主要ファイル
- [sources.yaml](sources.yaml)
- [schema.sql](schema.sql)
- [docker-compose.yml](docker-compose.yml)
- [Dockerfile](Dockerfile)
- [requirements.md](requirements.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/runbook.md](docs/runbook.md)
- [docs/assumptions.md](docs/assumptions.md)
