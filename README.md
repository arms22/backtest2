# backtest2

`backtest2` は、保存済み市場データを再生して取引ボットを検証するライブラリです。ボットや取引所への通信処理を含まず、Python 3.10 以上で動作します。各取引所のリポジトリから接続先を渡し、共通エンジンで約定と損益を計算します。

## 使い始める

各ボットのリポジトリから、隣接する `backtest2` ディレクトリをローカル依存として参照します。firebots と bankbots では任意の `backtest` extra を使います。各リポジトリのルートで実行してください。

```powershell
uv sync --extra backtest
uv run --extra backtest python -m backtest2 --help
```

実行には、保存データ、実験モジュール、出力先を指定します。`--experiment` には `build_cases()` を持つモジュールを指定します。モデルの既定値は `through` です。

```powershell
uv run --extra backtest python -m backtest2 `
  --experiment backtests.cfdmm `
  --data <保存データ> `
  --output <結果ディレクトリ>
```

## ボットとの接続

`build_cases()` は `BotCase` のリストを返します。必須項目は `name`、`create_bot`、`strategy_factory` です。エンジンが接続先を生成し、通常のボットへ Strategy として渡します。時計の差し替えと参照ソースは `clock_context` と `code_files` で指定します。

`asyncio_modules=(bot_module, strategy_module)` を指定すると、共通ランナーは再生中だけ対象モジュールの `asyncio` 参照を差し替え、`gather` を逐次実行します。指定しないケースは通常の並列実行です。共有の `asyncio` 自体は変更せず、再生終了時に各参照を元へ戻します。対象は `import asyncio` 形式の参照です。`from asyncio import gather` 形式には適用されません。

接続先は次の通知を実装します。

- `on_fill(order, quantity, price, kind)`: 共通台帳を更新した後に呼びます。
- `on_market_update()`: 各秒の約定・取消処理が終わった後に呼びます。

接続先は本番形式の注文情報と残高へ反映します。新規注文と取消にはエンジンの `submit_order()` と `cancel_order()` を使います。実装例は `../firebots/firebots/backtest.py` と `../bankbots/bankbots/backtest.py` です。

ボット固有の指標、板の深さ、注文数量、建玉制御は各ボット側に置きます。共通パッケージから firebots や bankbots を import しません。

## 入力データ

入力ディレクトリには `fetch_manifest.json` と `market_rows.jsonl.gz` を置きます。既存の firebots 用保存形式を引き継いでいます。取得処理は含みません。入力はあらかじめ単一商品に絞り込んでください。共通エンジンは商品識別子を扱わず、manifest の `product` や行の `product_code` を要求・参照しません。識別子はケース定義から `strategy_factory` に渡し、取引所アダプターが管理します。

共通処理が参照する保存行の列は次のとおりです。対象ボットやアダプターが追加で参照する列は、それぞれの実装で確認してください。

| 保存列 | 形式・用途 |
| --- | --- |
| `time` | タイムゾーン付きISO日時文字列。再生時刻とデータ間隔の算出に使います。 |
| `bids_raw`, `asks_raw` | `[価格, 数量]` 配列のJSON文字列。買い板は価格降順、売り板は価格昇順です。板参照、約定処理、清算評価、監査、描画に使います。 |
| `bid`, `ask` | 最良買い・売り価格の数値。中間価格とマークアウトの計算に使います。 |
| `low`, `high` | 区間の安値・高値。約定判定に使います。 |
| `buy_volume`, `sell_volume` | 区間の売買方向別数量。約定可能数量の計算に使います。 |
| `trades` | 区間の約定件数。約定判定に使います。 |
| `buy_count`, `sell_count` | 区間の売買方向別件数。共通の `Executions.get_data()` を使う場合に参照します。 |

読み込み時に `bids_raw` と `asks_raw` を展開して `bids` と `asks` を生成します。`timestamp` は `time` から得たUNIX秒、`second` はその秒未満を切り下げた値です。これらを入力ファイルへ追加する必要はありません。

データ間隔は、データ行の先頭2行の時刻を秒単位に切り下げ、その差から求めます。manifest の `interval_seconds` は間隔の判定に使いません。1秒、5秒、1分、1時間など整数秒の間隔に対応し、再生期間と分割境界はデータ間隔にそろえます。

共通処理は manifest の `raw_sha256` を使い、未指定または `null` の場合は `output.sha256` を使います。`missing_intervals_utc_seconds` と `invalid_rows` は省略できます。manifest に欠損区間または不正行が記録されている場合、入力を拒否します。各行の欠落・重複・時刻順序や末尾不足、板・OHLC・約定集計の値域や相互整合は検査しません。元のmanifestは書き換えません。

共通処理が使うファイル名を変更する場合は、`--data` からの相対パスまたは絶対パスを指定します。

```powershell
--data-file quotes.jsonl.gz --manifest-file source.json
```

Python API では `run_backtest(..., data_file=..., manifest_file=...)` を使います。指定したパスは結果に記録され、監査と描画も同じファイルを読みます。相対パスならデータディレクトリ全体を移動しても参照を維持できます。

参照ソースとして、パッケージ内のPythonファイル、実験ファイル、各ケースの `code_files` を記録します。ハッシュのキーは、参照ファイルすべての共通祖先からの相対パスです。複数リポジトリのファイルも指定できます。

## 再生モデルと制約

再生範囲は単一のJPY建て商品、初期建玉0、GTC指値、手数料0です。初期現金・残高制約・post-onlyの指定は取引所側の接続先が扱います。GMOの両建て・決済専用注文、成行、IOC/FOK、資金調達費、証拠金、ロスカットには対応しません。未対応の注文方式は接続先で明示的に拒否してください。

`wakeup()` は次の市場データレコードへ進みます。整数秒周期には `wakeup_every_seconds(interval)` を使います。各判断の秒内時刻は `decision_offset` で指定します。`sleep(seconds)` は指定時間以後の最初の保存レコードまで進むため、待機時間はデータ間隔の粒度に切り上がります。サブ秒のイベント順序は再現しません。

約定モデルは `touch`、`through`、`queue_touch` の3種類です。方向別出来高と板数量は注文間で共有します。post-only 注文が到着時の板を跨ぐ場合は拒否します。取消完了が集計区間内なら、その区間での約定を許します。3モデルを損益の上下限とはみなしません。実際の待ち順、API障害、通知遅延は再現しません。

## 結果と独立監査

結果ディレクトリには約定台帳、FIFO決済対応表、集計CSV、Markdownレポートを保存します。約定台帳 `*_fills.jsonl.gz` は約定イベントごとに台帳内連番の `fill_id`、UNIX秒の時刻、売買方向、数量、価格、Maker/Taker/終端決済の区別、注文ID、実現損益を記録します。

`*_matches.jsonl.gz` はFIFOで対応付けた決済片を記録します。各行にエントリー・決済の約定ID、注文ID、時刻、価格、方向、種別、数量、保有秒数、実現損益を含みます。同じ約定が複数の決済片に分かれる場合は別行になります。`*_open_lots.jsonl.gz` は終端清算の直前に残っていた建玉ロットを記録します。`exit_kind=terminal` の決済片は通常の決済と区別できます。

`match_summary.csv` は決済片を対象に、建玉方向、エントリー／決済の種別、エントリー／決済時刻（JST）の時間帯、保有時間帯別に集計します。勝率は損益が正の決済片数を全決済片数で割り、平均損益も決済片単位です。売買代金はエントリーと決済の両方を含みます。

`results.json` は正常終了時に最後に作成され、完了した実行を示します。約定後のマークアウトは1、5、30、60秒後とデータ間隔後の中間価格を使い、約定方向別およびMaker/Taker別にも集計します。目標時刻に完全一致するレコードだけを使い、別時刻の価格で補いません。観測対象期間内で目標時刻のレコードがない場合の欠損率と、データ末尾で観測窓がない約定数を分けて記録します。粗いデータ間隔では短い時間幅の観測率が低くなることがあります。

出力先がすでに存在する場合、実行は停止します。置き換えるには `--overwrite` を指定します。出力先ディレクトリ全体を削除して作り直すため、以前のグラフ、監査結果、独自ファイルも削除されます。入力データ、作業ディレクトリ、参照ソースを含む場所や、リンクされたディレクトリは上書きできません。

独立監査は、保存台帳と元の板から損益を再計算し、保存したFIFO決済対応表も別経路で再構成して照合します。assertで照合するため、`python -O` を使わないでください。

```powershell
uv run --extra backtest python -m backtest2.audit `
  --data <保存データ> `
  --results <結果ディレクトリ>
```

## 損益グラフ

グラフは完了済みの `results.json`、約定台帳、元の保存データから描画します。ボットの再実行やデータの再取得は行いません。描画時のみ `matplotlib` が必要です。各ボットのリポジトリから実行してください。

```powershell
uv run --extra backtest --with matplotlib python -m backtest2.plot `
  --data <保存データ> `
  --results <結果ディレクトリ> `
  --utc-offset 9
```

既定の出力先は `<結果ディレクトリ>/pnl.svg` です。`--output <ファイル.png>` のように指定すると、形式と出力先を変更できます。`--case <ケース名>` と `--model <モデル名>` は複数指定でき、省略時はすべてのケースとモデルを描画します。モデルごとにパネルを分け、凡例に最終損益を表示します。時刻は既定でUTCです。日本時間を表示するには `--utc-offset 9` を指定します。

縦軸は現金増減に未決済建玉の清算価値を加えたJPY損益です。各バーの板で評価します。粗い間隔ではバー内の価格や注文順序を復元できず、約定モデルは集計OHLCVに基づく近似になります。描画時には入力ハッシュと各区間の最終損益・建玉を保存結果と照合します。独立して再生した区間は、前区間の最終損益を加算してつなぎます。

## 開発時の確認

backtest2 リポジトリのルートで実行します。テストは模擬データだけを使います。

```powershell
uv run python -m unittest discover -s tests -v
uv run python -m compileall -q backtest2
```

uv のキャッシュ先は `pyproject.toml` の `[tool.uv]` で `.cache/uv` に設定済みです。`UV_CACHE_DIR` を指定する必要はありません。`PYTHONPYCACHEPREFIX` は指定せず、Python標準の `__pycache__/` を使います。キャッシュは `.gitignore` で除外しています。
