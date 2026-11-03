"""Read and validate saved single-product market snapshots; no network access."""
import gzip
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path


def iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace('+00:00', 'Z')


def parse_time(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.microsecond:
        raise ValueError('Specify a timezone and whole seconds')
    return int(stamp.timestamp())


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def get_interval_seconds(directory, manifest, data_file='market_rows.jsonl.gz'):
    """Read the first two timestamps to infer a fixed whole-second interval."""
    with gzip.open(Path(directory)/data_file, 'rt', encoding='utf-8') as source:
        try:
            first = json.loads(next(source))
            second = json.loads(next(source))
        except StopIteration:
            raise ValueError('At least two market rows are needed to infer the interval') from None
    first_time = datetime.fromisoformat(first['time'].replace('Z', '+00:00'))
    second_time = datetime.fromisoformat(second['time'].replace('Z', '+00:00'))
    if first_time.tzinfo is None or second_time.tzinfo is None:
        raise ValueError('Market record has no timezone')
    interval = math.floor(second_time.timestamp()) - math.floor(first_time.timestamp())
    if interval < 1:
        raise ValueError('Backtest requires fixed whole-second OHLCV intervals')
    return interval


def load_market_manifest(directory, data_file='market_rows.jsonl.gz', manifest_file='fetch_manifest.json'):
    directory = Path(directory)
    manifest = json.loads((directory/manifest_file).read_text(encoding='utf-8'))
    if manifest.get('missing_intervals_utc_seconds') or manifest.get('invalid_rows'):
        raise ValueError('Market data contains missing or invalid rows')
    expected_hash = manifest.get('raw_sha256')
    if expected_hash is None:
        expected_hash = manifest.get('output', {}).get('sha256')
    if not isinstance(expected_hash, str) or not expected_hash:
        raise ValueError('Manifest has no data SHA256')
    if file_hash(directory/data_file) != expected_hash:
        raise ValueError('Raw data hash does not match the manifest')
    if parse_time(manifest['end_utc_exclusive']) <= parse_time(manifest['start_utc']):
        raise ValueError('Invalid market-data period')
    # Normalize in memory for runner/plot consumers; never rewrite the source manifest.
    return dict(manifest, raw_sha256=expected_hash,
                interval_seconds=get_interval_seconds(directory, manifest, data_file))


def validate_market_row(row):
    bids, asks = (json.loads(row[k]) for k in ('bids_raw', 'asks_raw'))
    if not bids or not asks:
        raise ValueError('Empty book')
    if not all(math.isfinite(p) and math.isfinite(q) and p > 0 and q > 0
               for p, q in bids + asks):
        raise ValueError('Nonpositive/nonfinite book')
    if any(a[0] <= b[0] for a, b in zip(bids, bids[1:])):
        raise ValueError('Bids must be strictly descending')
    if any(a[0] >= b[0] for a, b in zip(asks, asks[1:])):
        raise ValueError('Asks must be strictly ascending')
    if bids[0][0] >= asks[0][0] or (bids[0][0], asks[0][0]) != (row['bid'], row['ask']):
        raise ValueError('Crossed/mismatched book')
    for key in ('open', 'high', 'low', 'close', 'buy_volume', 'sell_volume',
                'buy_count', 'sell_count', 'trades', 'delay'):
        if not isinstance(row.get(key), (int, float)) or not math.isfinite(row[key]):
            raise ValueError('Missing/nonfinite aggregate: ' + key)
    if not (0 < row['low'] <= min(row['open'], row['close']) <=
            max(row['open'], row['close']) <= row['high']):
        raise ValueError('Invalid OHLC')
    if min(row[k] for k in ('buy_volume', 'sell_volume', 'buy_count', 'sell_count', 'trades')) < 0:
        raise ValueError('Negative activity')
    if row['buy_count'] + row['sell_count'] != row['trades']:
        raise ValueError('Trade counts disagree')
    for side in ('buy', 'sell'):
        if (row[side + '_count'] == 0) != (row[side + '_volume'] == 0):
            raise ValueError('Count/volume disagree')
    return bids, asks


def iter_market_rows(directory, start, end, data_file='market_rows.jsonl.gz', interval_seconds=1):
    """Decode stored market data without row-level validation."""
    with gzip.open(Path(directory)/data_file, 'rt', encoding='utf-8') as source:
        for line in source:
            row = json.loads(line)
            stamp = datetime.fromisoformat(row['time'].replace('Z', '+00:00'))
            timestamp = stamp.timestamp()
            second = math.floor(timestamp)
            if second < start:
                continue
            if second >= end:
                break
            # Replay consumes the stored values like the bot consumes exchange
            # data; only decode the book fields needed by the strategy/model.
            row['bids'] = json.loads(row['bids_raw'])
            row['asks'] = json.loads(row['asks_raw'])
            row['timestamp'], row['second'] = timestamp, second
            yield row
