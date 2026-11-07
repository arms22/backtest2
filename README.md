# backtest2

`backtest2` は、保存済みの市場データを再生して取引ボットを検証するライブラリです。Python 3.10 以上で動作し、ボット本体や取引所への通信処理は含みません。各取引所向けのボットリポジトリから取引所アダプターを渡し、共通エンジンで約定と損益を計算します。

## 使い始める

各ボットのリポジトリから、隣接する `backtest2` ディレクトリをローカル依存として参照します。firebots と bankbots では、任意で導入する追加の依存関係をまとめた `backtest` extra を使います。次のコマンドを各ボットリポジトリのルートで実行してください。

```powershell
uv sync --extra backtest
uv run --extra backtest python -m backtest2 --help
```

実行時には、保存データ、実験モジュール、出力先を指定します。`--experiment` には `build_cases()` を持つモジュールを指定します。CLIとPython APIの約定モデルの既定値は `queue_touch` です。

```powershell
uv run --extra backtest python -m backtest2 `
  --experiment backtests.cfdmm `
  --data <保存データ> `
  --output <結果ディレクトリ>
```

## ボットとの接続

`build_cases()` は `BotCase` のリストを返します。必須項目は `name`、`create_bot`、`strategy_factory` です。エンジンが取引所アダプターを生成し、通常のボットへ Strategy として渡します。時計の差し替えは `clock_context`、参照するソースファイルは `code_files` で指定します。

`asyncio_modules=(bot_module, strategy_module)` を指定すると、共通の実行処理（ランナー）が再生中だけ対象モジュールの `asyncio` 参照を差し替え、`gather` を逐次実行します。省略時は通常の並列実行です。共有の `asyncio` 自体は変更せず、再生終了時に各参照を元へ戻します。

差し替えの対象は `import asyncio` 形式の参照です。`from asyncio import gather` 形式には適用されません。

取引所アダプターは、次の通知を受け取るメソッドを実装します。

- `on_fill(order, quantity, price, kind)`: 共通台帳の更新後に呼び出されます。
- `on_market_update()`: 保存データの各行に対する約定・取消処理後に呼び出されます。

取引所アダプターは、通知の内容を本番と同じ形式の注文情報と残高に反映します。新規注文にはエンジンの `submit_order()`、取消には `cancel_order()` を使います。実装例は `../firebots/firebots/backtest.py` と `../bankbots/bankbots/backtest.py` です。

ボット固有の指標、板の深さ、注文数量、建玉制御は各ボット側に置きます。

## 再生中の市場データを取得する

各取引所のバックテスト用 Strategy では、`await strategy.get_market_data(...)` で現在の保存行を辞書として取得できます。本番の Strategy には、このメソッドはありません。

| 対象 | 呼び出し |
| --- | --- |
| bankbots | `await strategy.get_market_data(pair=None)` |
| coinbots | `await strategy.get_market_data(pair)` |
| firebots | `await strategy.get_market_data(product_code)` |
| gmobots | `await strategy.get_market_data(symbol=None)` |

省略可能な商品引数は設定済みの商品を使います。設定外の商品を指定すると `ValueError` になります。

```python
await strategy.wakeup()
data = await strategy.get_market_data('btc_jpy')  # bankbots / coinbots の例
buy_volume = data['buy_volume']
sell_volume = data['sell_volume']
```

保存行の列名・値・型を維持し、取引所固有の列もそのまま返します。欠けている列の補完や個別約定への変換は行いません。読み込み処理が追加した内部項目は含めません。たとえば `bids_raw` はJSON文字列のままです。

`time` は集計区間の終了時刻です。現在の再生行を確定済みのデータとして公開し、追加で1区間遅らせることはありません。返却値は入れ子の値もコピーするため、変更しても再生処理や次の取得結果には影響しません。最初の市場データが届く前は `RuntimeError` になるため、`wakeup()` などで最初の更新を待ってください。

## 入力データ

入力ディレクトリには `fetch_manifest.json` と `market_rows.jsonl.gz` を置きます。

入力データは、取得時に検証対象の単一商品へ絞り込んでください。共通エンジンは、商品識別子によるデータの選別を行いません。取引所アダプターが使う商品識別子は、ケース定義から `strategy_factory` へ渡します。

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

読み込み時に `bids_raw` と `asks_raw` を展開して `bids` と `asks` を生成します。`timestamp` は `time` から得たUNIX秒、`second` はその秒未満を切り下げた値です。

データ間隔は、データ行の先頭2行の時刻を秒単位に切り下げ、その差から求めます。manifest の `interval_seconds` は間隔の判定に使いません。1秒、5秒、1分、1時間など整数秒の間隔に対応し、再生期間と分割境界はデータ間隔にそろえます。

入力ハッシュには manifest の `raw_sha256` を使います。未指定または `null` の場合は `output.sha256` を使います。`missing_intervals_utc_seconds` と `invalid_rows` は省略できます。manifest に欠損区間または不正行が記録されている場合、入力は拒否されます。

共通処理が使うファイル名を変更する場合は、`--data` からの相対パスまたは絶対パスを指定します。

```powershell
--data-file quotes.jsonl.gz --manifest-file source.json
```

Python API では `run_backtest(..., data_file=..., manifest_file=...)` を使います。指定したパスは結果に記録され、監査と描画も同じファイルを読みます。相対パスならデータディレクトリ全体を移動しても参照を維持できます。

参照するソースファイルとして、パッケージ内のPythonファイル、実験ファイル、各ケースの `code_files` を記録します。ハッシュのキーには、すべての参照ファイルに共通する最も近い上位ディレクトリからの相対パスを使います。複数リポジトリのファイルも指定できます。

## 再生モデルと制約

JPY建ての単一商品を、初期建玉0、手数料0で再生します。対象の注文方式はGTC指値です。初期現金、残高制約、post-onlyの指定は取引所アダプターが扱います。

GMOの両建て・決済専用注文、成行、IOC/FOK、資金調達費、証拠金、ロスカットには対応しません。未対応の注文方式は取引所アダプターで明示的に拒否してください。

`wakeup()` は次の市場データレコードへ進みます。整数秒周期には `wakeup_every_seconds(interval)` を使います。各判断の秒内時刻は `decision_offset` で指定します。`sleep(seconds)` は指定時間以後の最初の保存レコードまで進むため、待機時間はデータ間隔の粒度に切り上がります。サブ秒のイベント順序は再現しません。

約定モデルは `touch`、`through`、`queue_touch` の3種類です。方向別出来高と板数量は注文間で共有します。post-only 注文が到着時に板を跨ぐ場合、その注文は拒否されます。取消が集計区間内に完了する場合も、その区間中に約定した可能性を認めます。

各モデルの結果は、損益の上限・下限を保証しません。実際の注文の待ち順位、API障害、通知遅延は再現しません。

## 結果と独立監査

結果ディレクトリには約定台帳、FIFO決済対応表、集計CSV、Markdownレポートを保存します。約定台帳 `*_fills.jsonl.gz` は約定イベントごとに台帳内連番の `fill_id`、UNIX秒の時刻、売買方向、数量、価格、Maker/Taker/終端決済の区別、注文ID、実現損益を記録します。

`*_matches.jsonl.gz` は、先入先出（FIFO）方式で対応付けた建玉と決済の数量単位（決済片）を記録します。各行にエントリー・決済の約定ID、注文ID、時刻、価格、方向、種別、数量、保有秒数、実現損益を含みます。同じ約定が複数の決済片に分かれる場合は別行になります。`exit_kind=terminal` の決済片は通常の決済と区別できます。

`*_open_lots.jsonl.gz` は、終端清算の直前に残っていた建玉ロットを記録します。

`match_summary.csv` は決済片を対象に、建玉方向、エントリー時と決済時の約定種別、エントリー時刻と決済時刻のJST時間帯、保有時間帯別に集計します。勝率は、損益が正の決済片数を全決済片数で割った値です。平均損益も決済片単位です。売買代金はエントリーと決済の両方を含みます。

`results.json` は正常終了時に最後に作成され、実行が完了したことを示します。

`results.json` と `summary.csv` には、買い・売り別の発注数（`buy_orders`、`sell_orders`）、約定した注文数（`buy_filled_orders`、`sell_filled_orders`）、約定率（`buy_fill_rate`、`sell_fill_rate`）を保存します。約定率は「一度でも約定した注文数 ÷ 発注数」で、0〜1の値です。部分約定も1注文として数え、同じ注文の複数回の約定は重複して数えません。発注数には発注後に拒否・取消された注文と終了時の未約定注文を含め、ウォームアップ中に抑止した注文と終端清算は除外します。発注数が0なら約定率はJSONで `null`、CSVで空欄です。`REPORT.md` では百分率で表示します。注文履歴も既定で保存します。

約定後のマークアウトは、入力データの間隔に応じた時間幅で計算します。

| データ間隔の例 | 約定からの時間幅 |
| --- | --- |
| 1秒、5秒 | 1、5、30、60秒 |
| 1分 | 1、5、30、60分 |
| 1時間 | 1、5、30、60時間 |

一般には、データ間隔が1分未満なら秒、1時間未満なら分、それ以上なら時間を単位に1、5、30、60を選び、データ間隔そのものも加えます。

目標時刻は、約定時刻に各時間幅を加えた時刻です。目標時刻以降の最初の保存行にある中間価格を使い、実際の観測時刻と約定からの経過時間を記録します。データ間隔や欠落行によっては、指定した時間幅より遅れて観測されます。保存データの末尾までに目標時刻以降の行がない場合は、観測を打ち切ります。

マークアウトの集計値は `results.json` に保存します。約定ごとの値は `*_fill_markouts.jsonl.gz` に保存し、各行に約定ID、売買方向、約定種別、価格、数量、時間幅ごとの中間価格・マークアウト・観測状態を記録します。終端清算約定は集計・保存の対象外です。

出力先がすでに存在する場合、実行は停止します。置き換えるには `--overwrite` を指定します。出力先ディレクトリ全体を削除して作り直すため、以前のグラフ、監査結果、独自ファイルも削除されます。入力データ、作業ディレクトリ、参照ソースを含む場所や、リンクされたディレクトリは上書きできません。

独立監査では、保存済みの台帳と元の板から損益を再計算します。FIFO決済対応表も別の処理で再構成し、保存済みの対応表と照合します。照合には `assert` を使うため、`python -O` を使わないでください。

```powershell
uv run --extra backtest python -m backtest2.audit `
  --data <保存データ> `
  --results <結果ディレクトリ>
```

## 保存済み台帳の活動量集計

`backtest2.analysis.summarize_activity(fills, matches, fast_seconds=60)` は、通常決済の利益合計・損失合計、終了決済の損益、終了決済を除く買い数量、通常決済の数量加重平均保有秒数、短時間売却割合を辞書で返します。買い数量のキーは `buy_quantity` です。短時間売却割合 `fast_fraction` は「保有時間が `fast_seconds` 未満の通常決済数量 ÷ 買い数量」で、買い持ちの検証で使う定義です。比率の分母が0なら `None`、合計の対象がなければ0を返します。

```python
from backtest2.analysis import load_audited_results, read_jsonl_gz, summarize_activity

saved = load_audited_results(directory)
case = saved['results'][0]
prefix = directory / (case['segment'] + '_' + case['name'])  # directory は Path
stats = summarize_activity(
    read_jsonl_gz(str(prefix) + '_fills.jsonl.gz'),
    read_jsonl_gz(str(prefix) + '_matches.jsonl.gz'),
)
```

`load_audited_results` は `results.json` と `audit_checks.json` のSHA256参照を照合します。台帳や入力データの再監査は行いません。分類別の決済片集計には既存の `summarize_matches` を使います。

`summarize_activity` は終了決済を除く売り約定数量 `sell_quantity` も返します。CFDの買い戻しを含むため、買い・売り約定数量は新規建玉数量とは異なります。買い建玉と売り建玉を分ける場合は、次のようにFIFO決済対応表のエントリー方向で集計します。

```python
from backtest2.analysis import summarize_position_activity

long_stats = summarize_position_activity(matches, entry_side='BUY')
short_stats = summarize_position_activity(matches, entry_side='SELL', fast_seconds=60)
```

方向別の返却値は `entry_quantity`、`normal_profit`、`normal_loss`、`terminal_pnl`、`holding_seconds`、`fast_fraction` です。`entry_quantity` は通常決済と終了決済に対応する新規建玉の数量で、終了決済としてのエントリーは除外します。`fast_fraction` は「閾値未満で通常決済された数量 ÷ entry_quantity」で、売り建玉では短時間で買い戻した割合です。平均保有時間は通常決済だけの数量で加重します。対象なし・分母0の扱いは全体集計と同じです。未決済ロットは含まないため、全建玉を集計する際は終了決済済みの完了結果を使ってください。既存の全体集計の `fast_fraction` は従来どおり買い約定数量を分母にします。

## 保存済み注文の分析

既定で `*_orders.jsonl.gz` に1注文1行の要約を保存します。各結果の `orders_file` が保存先、`order_summary` が売買別集計です。ケース・期間区間・モデル・遅延の分離は約定台帳と同じで、`order` を同じ結果の約定台帳に対応付けます。IDだけで別の結果を結合しないでください。既存の名前が重複する設定は拒否されるため、取消遅延だけが異なる比較などは別ディレクトリで実行します。

対象はエンジンが受け付けた注文で、未約定取消、部分約定、到着時のpost-only拒否、期限切れ、期間末の残存を含みます。アダプター内での拒否、ウォームアップで抑止した注文、終端清算は含みません。方向・指値・発注数量・約定数量・残数量・状態と、判断時の `bid`・`ask` を記録します。拒否時も残数量は発注数量から約定数量を引いた値です。

時刻はUNIX秒です。`decision` は発注判断、`arrival` は判断＋設定遅延、`cancel_requested` は最初の取消要求です。`cancelled_at` は再生中に取消完了を処理した場合の有効な完了時刻で、期限切れは `expired_at` に分けます。`first_fill`・`last_fill` は台帳の最初・最後の約定時刻で、個々の部分約定は台帳を参照します。区間内の取消より先に約定した可能性を認める既存モデルにより、約定の保存時刻が取消完了時刻より後になる場合があります。

`cutoff` は最後に観測した市場データの時刻、`open_at_cutoff` は期間末の残存を表します。残存状態は `pending`・`active`・`canceling` のままで、終端処理を取消完了には数えません。未観測時刻は `null` です。保存量とメモリ使用量は注文数に比例します。CLIの `--no-order-history`、または `ReplayConfig(record_orders=False)` で保存を無効にできます。

```python
import json
from pathlib import Path
from backtest2.analysis import read_jsonl_gz, summarize_orders

# case は results.json の results 内の1件、directory は結果ディレクトリ
stats = summarize_orders(read_jsonl_gz(Path(directory) / case['orders_file']))
detail = Path(directory) / (case['segment'] + '_' + case['name'] + '_order_analysis.json')
detail.write_text(json.dumps(stats, indent=2), encoding='utf-8')
print('| Side | Orders | Filled | Unfilled cancelled |')
print('| --- | ---: | ---: | ---: |')
for side, values in stats.items():
    print(f"| {side} | {values['orders']} | {values['filled_orders']} | {values['unfilled_cancelled']} |")
```

`summarize_orders` は売買別の注文数、一度以上約定した注文数、未約定取消数、状態別件数を返します。`cancel_request_seconds`・`cancel_complete_seconds`・`first_fill_seconds` は判断から各イベントまでの秒数、`order_interval_seconds` は同方向の判断時刻順の発注間隔です。`distance_mid_bp` は買いで `(mid-price)/mid*10000`、売りで `(price-mid)/mid*10000` です。各分布は観測数 `count` と線形補間の `p50`・`p90`・`p99` を返し、対象なしの分位点は `None` です。未観測時刻は分布から除外するため、全注文の寿命とは区別してください。古い結果や保存無効時の履歴は復元できません。

## 損益グラフ

グラフは完了済みの `results.json`、約定台帳、元の保存データから描画します。描画時のみ `matplotlib` が必要です。各ボットのリポジトリから実行してください。

```powershell
uv run --extra backtest --with matplotlib python -m backtest2.plot `
  --data <保存データ> `
  --results <結果ディレクトリ> `
  --utc-offset 9
```

既定の出力先は `<結果ディレクトリ>/pnl.svg` です。`--output <ファイル.png>` のように指定すると、形式と出力先を変更できます。`--case <ケース名>` と `--model <モデル名>` は複数指定でき、省略時はすべてのケースとモデルを描画します。モデルごとにパネルを分け、凡例に最終損益を表示します。時刻は既定でUTCです。日本時間を表示するには `--utc-offset 9` を指定します。

タイトルは `--title "FX_BTC_JPY cumulative P&L"` のように指定します。省略時は `Cumulative P&L` です。Python APIでは `plot_pnl(..., title="FX_BTC_JPY cumulative P&L")` を使います。

縦軸は、現金増減に未決済建玉の清算価値を加えたJPY損益です。清算価値は各バーの板で評価します。データ間隔が粗いと、バー内の価格や注文順序を復元できず、約定モデルは集計OHLCVに基づく近似になります。

描画時には、入力ハッシュと各区間の最終損益・建玉を保存結果と照合します。独立して再生した区間は、前区間の最終損益を加算してつなぎます。

## 開発時の確認

backtest2 リポジトリのルートで実行します。テストは模擬データだけを使います。

```powershell
uv run python -m unittest discover -s tests -v
uv run python -m compileall -q backtest2
```

uv のキャッシュ先は `pyproject.toml` の `[tool.uv]` で `.cache/uv` に設定済みです。`PYTHONPYCACHEPREFIX` は指定せず、Python標準の `__pycache__/` を使います。キャッシュは `.gitignore` で除外しています。
