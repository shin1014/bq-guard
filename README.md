# bq-guard

BigQuery のクエリを TUI で安全に編集・実行するためのガード付きアプリです。dry-run の見積もり結果をレビュー・承認した場合のみ実行できます。

## セットアップ (uv)

```bash
uv venv
uv pip install -e .
```

社内配布 (Git tag 指定):

```bash
uv tool install "git+ssh://git@github.com/<OWNER>/<REPO>.git@vX.Y.Z"
```

アップデート:

```bash
uv tool upgrade bq-guard
```

アンインストール:

```bash
uv tool uninstall bq-guard
```

## 起動

```bash
bq-guard
```

または

```bash
python -m bq_guard
```

## 認証 (ADC)

```bash
gcloud auth application-default login
```

## 設定ファイル

`~/.config/bq_guard/config.yaml` に生成されます。初回起動時にデフォルト設定が書き出されます。

## 主な操作

- Ctrl+E: dry-run 見積もり
- Ctrl+Enter: Review/Approve フロー開始 (実行は承認後のみ)
- Ctrl+S: CSV エクスポート
- Ctrl+, (comma): 設定編集
- Ctrl+M: テーブルメタ再取得
- Ctrl+Q: 終了
