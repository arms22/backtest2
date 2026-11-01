# backtest2

取引ボットから独立した、保存済み市場データの再生ライブラリ。外部依存はなく、Python 3.10以上で動作する。取引所への通信処理は持たない。

## 分担

| 配置 | 担当 |
| --- | --- |
| `backtest2/data.py` | 保存データ、ハッシュ・欠損・板・OHLCの検査 |
| `backtest2/replay.py` | 仮想時計、GTC指値の約定・取消、終端決済、リスク集計 |
| `backtest2/accounting.py` | 初期建玉0・手数料0のFIFO損益 |
| `backtest2/runner.py` | ボット生成と接続先の受取、ケース比較、期間分割、結果出力 |
| `backtest2/audit.py` | 保存台帳と元の板による独立再計算 |
| 各リポジトリの `backtest.py` | 本番Strategyの注文・取消と、取引所固有の注文情報・残高の接続 |
| 各リポジトリの `backtests/` | ボット生成、設定、時計の接続、参照ソースの指定 |

このパッケージからfirebots・bankbotsなどをimportしない。ボットの指標、深さ、数量、建玉制御は各ボットに残す。

## 利用

各ボットのリポジトリから、隣接するこのディレクトリをローカル依存として参照する。firebotsとbankbotsには任意の `backtest` extraを設定している。通常の実行環境に追加する場合は、各リポジトリのルートで実行する。

```powershell
$env:UV_CACHE_DIR = Join-Path $PWD '.uv-cache'
uv sync --extra backtest
uv run --extra backtest python -m backtest2 --help
```

`--experiment` で指定したモジュールの `build_cases()` から `BotCase` のリストを受け取る。必須項目は `name`、`create_bot`、`strategy_factory`。接続先はエンジンから生成し、通常のボットへStrategyとして渡す。時計の差替えと参照ソースは、ケースの `clock_context` と `code_files` で指定する。

接続先が実装する通知は `on_fill(order, quantity, price, kind)` と `on_market_update()` の2つ。前者は共通台帳の更新後、後者は各秒の約定・取消処理の完了後に呼ぶ。接続先は本番形式の注文情報・残高へ反映する。新規・取消はエンジンの `submit_order()`・`cancel_order()` を使用する。対応例は `../firebots/firebots/backtest.py` と `../bankbots/bankbots/backtest.py`。

入力は `fetch_manifest.json` と `market_rows.jsonl.gz`。1商品・連続した1秒ごとの板、OHLC、売買方向別数量・件数を使用する。既存のfirebots用保存形式を引き継いでいる。別の取引所のデータは商品名・取得条件とともに同形式へ書き出す。取得処理はこのライブラリに含めない。

ソースの記録はパッケージ内のPythonファイル、実験ファイル、`code_files` が対象。ハッシュのキーは全参照ファイルの共通祖先からの相対パスであり、複数リポジトリを参照できる。参照ソースはケース側で明示する。

## 現在の範囲

単一のJPY建て商品、初期建玉0、GTC指値、手数料0の再生に絞っている。現物の初期現金・残高制約・post-onlyの指定は取引所側の接続先が扱う。GMOの両建て・決済専用注文、成行、IOC/FOK、資金調達費、証拠金・ロスカットは未対応。未対応の注文方式は接続先で明示的に拒否する。

`wakeup()` は次の1秒レコードへ進める。従来のfirebots評価の固定時刻接続を維持する。整数秒周期には `wakeup_every_seconds(interval)` を使う。各判断の秒内時刻は `decision_offset` で指定する。`sleep(seconds)` は期限以後の最初の保存秒まで進むため、0.5秒待機などは1秒データの粒度へ切り上がる。サブ秒のイベント順序は再現しない。

touch・through・queue_touchの3モデルを持つ。方向別出来高と板数量を注文間で共有する。post-only注文が到着時の板を跨ぐ場合は拒否する。取消完了が集計区間内なら、その区間での約定を許す。3モデルを損益の上下限とは扱わない。実際の待ち順・API障害・通知遅延は未再現。

出力先は新しいディレクトリを指定する。`results.json` は完了時だけ作る。約定台帳、CSV、Markdownも保存する。独立監査は次の入口を使う。assertで照合するため、`python -O` は使わない。

```powershell
uv run --extra backtest python -m backtest2.audit --data <保存データ> --results <結果>
```

## 開発確認

このリポジトリのルートで実行する。テストは模擬データだけを使う。

```powershell
$env:UV_CACHE_DIR = Join-Path $PWD '.uv-cache'
uv run python -m unittest discover -s tests -v
uv run python -m compileall -q backtest2
```
