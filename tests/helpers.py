import gzip
import json

from backtest2.data import file_hash, iso
from backtest2.replay import wakeup


def market(second, **changes):
    row = dict(second=second, timestamp=second+.01, bids=[[90., .1]], asks=[[120., .1]],
               bid=90., ask=120., open=99., high=99., low=98., close=98., delay=.1,
               sell_volume=.006, buy_volume=0., sell_count=1, buy_count=0, trades=1)
    row.update(changes)
    return row


class Adapter:
    def __init__(self, engine):
        self.engine = engine

    def on_fill(self, order, quantity, price, kind):
        pass

    def on_market_update(self):
        pass


class PassiveBot:
    async def run(self, strategy):
        while True:
            await wakeup()


def save_market(directory, seconds):
    with gzip.open(directory/'market_rows.jsonl.gz', 'wt', encoding='utf-8') as target:
        for second in seconds:
            row = market(second)
            row['time'] = iso(row['timestamp'])
            row['bids_raw'] = json.dumps(row.pop('bids'))
            row['asks_raw'] = json.dumps(row.pop('asks'))
            target.write(json.dumps(row)+'\n')
    manifest = dict(start_utc=iso(2000), end_utc_exclusive=iso(2004),
                    missing_intervals_utc_seconds=[], invalid_rows=[],
                    raw_sha256=file_hash(directory/'market_rows.jsonl.gz'))
    (directory/'fetch_manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
