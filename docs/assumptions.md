# Assumptions

## Confirmed from public sources
- JEPX spot は `/_download.php` への POST で年度 CSV を取得できることを確認済みです。
- JEPX intraday は `/_download.php` への POST で年度 CSV を取得できることを確認済みです。
- OCCTO 広域予備率は `https://web-kohyo.occto.or.jp/kks-web-public/home/download` で ZIP を取得できることを確認済みです。
- 参考リポジトリ `hama-jp/occto-grid-observatory` が OCCTO intertie / trunk / generation の公開画面 selector を用いていることを確認済みです。

## Unverified but implemented as reasonable public-site assumptions
- OCCTO 地域間連系線潮流、地内基幹送電線潮流、ユニット別発電実績は、この作業環境に Python 実行系と Playwright 実行環境がないため live download までは再確認できていません。
- 上記 3 系統の browser selector は、公開画面 HTML と参考リポジトリの最新実装に合わせています。
- OCCTO intertie のエリア対応は参考リポジトリにあわせた固定マッピングです。公開サイト側の名称変更時は再確認が必要です。

## Public spec unclear / manual confirmation required
- JEPX `時刻コード` を `market_ts_jst` の開始時刻として扱っています。JEPX 利用部門で終了時刻解釈を使う場合は API 利用側で合わせてください。
- OCCTO generation の `00:30[kWh] ... 24:00[kWh]` は公開 CSV 表記どおりの時刻ラベルで `market_ts_jst` に展開しています。
- OCCTO reserve ZIP には連系線 CSV も含まれますが、本 PoC の reserve source では block reserve CSV のみを正規化対象にしています。

## Explicit non-goals kept out of implementation
- 非公開データ
- 火力内部制約推定
- SOC 推定
- 予測 AI
- 高度 GIS

## Review checklist kept
- 公開データ前提で動く構成か: はい。ただし browser source は live 実行再確認が残ります。
- DOM 変更に脆弱すぎないか: HTTP 優先、browser 部は selector を最小にしています。
- raw 保存と監査性があるか: `data/raw` と `source_snapshot` に保持します。
- PoC として過不足がないか: 収集、品質、DB、API、自動実行まで含みます。
- 将来の本番化に耐える最小設計か: narrow fact / snapshot audit / UPSERT / backfill 前提です。
