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


def load_market_manifest(directory):
    directory = Path(directory)
    manifest = json.loads((directory/'fetch_manifest.json').read_text(encoding='utf-8'))
    if manifest['missing_intervals_utc_seconds'] or manifest['invalid_rows']:
        raise ValueError('Market data contains missing or invalid rows')
    if file_hash(directory/'market_rows.jsonl.gz') != manifest['raw_sha256']:
        raise ValueError('Raw data hash does not match the manifest')
    if parse_time(manifest['end_utc_exclusive']) <= parse_time(manifest['start_utc']):
        raise ValueError('Invalid market-data period')
    return manifest


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


def iter_market_rows(directory, start, end, product_code):
    previous = start - 1
    with gzip.open(Path(directory)/'market_rows.jsonl.gz', 'rt', encoding='utf-8') as source:
        for line in source:
            row = json.loads(line)
            stamp = datetime.fromisoformat(row['time'].replace('Z', '+00:00'))
            if stamp.tzinfo is None:
                raise ValueError('Market record has no timezone')
            timestamp = stamp.timestamp()
            second = math.floor(timestamp)
            if second < start:
                continue
            if second >= end:
                break
            if second != previous + 1:
                raise ValueError('Missing, duplicate or unordered market seconds')
            if row.get('product_code', product_code) != product_code:
                raise ValueError('Mixed products in a single-product replay')
            row['bids'], row['asks'] = validate_market_row(row)
            row['timestamp'], row['second'] = timestamp, second
            previous = second
            yield row
    if previous != end - 1:
        raise ValueError('Incomplete market-data period')
