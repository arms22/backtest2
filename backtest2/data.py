"""Read and validate saved single-product market snapshots; no network access."""
import gzip
import hashlib
import json
import math
import re
from collections import UserList
from datetime import datetime, timezone
from functools import cached_property
from itertools import islice
from pathlib import Path

from msgspec import DecodeError
from msgspec.json import decode


def decode_market_json(value):
    try:
        return decode(value)
    except (DecodeError, UnicodeError):
        # Older snapshots can contain NaN and other values accepted by json.loads.
        return json.loads(value)


class MarketRow(dict):
    """Replay fields with the original saved columns kept separately."""
    def __init__(self, source):
        super().__init__(source)
        self.source = source

    @cached_property
    def raw_trades(self):
        """Return validated trades in saved order, or None for a missing column."""
        raw = self.get('trades_raw')
        if raw is None:
            return None
        if not isinstance(raw, str):
            raise ValueError('trades_raw must be a JSON array encoded as a string')
        if raw == '[]':
            return ()
        trades = decode_market_json(raw)
        if not isinstance(trades, list):
            raise ValueError('trades_raw must be a JSON array')
        for offset, side, price, size in trades:
            if (type(offset) is not int or type(side) is not int or side not in (-1, 1)
                    or not all(math.isfinite(value) and value > 0 for value in (price, size))):
                raise ValueError('Invalid trades_raw execution')
        return tuple(tuple(trade) for trade in trades)


# Stored levels are flat [price, size] arrays. Leave uncommon layouts to the
# regular decoder. The final comma proves there is a tail after level 20.
_BOOK_PREFIX = re.compile(r'\s*\[\s*(?:\[[^\[\]"]*\]\s*,\s*){20}')
_NEXT_LEVEL = re.compile(r'(\[[^\[\]"]*\])\s*,\s*')


class _ReplayBook(UserList):
    """Decode the first 20 levels on access and the tail only when needed."""

    def __init__(self, raw):
        self._raw = raw
        self._levels = list(raw) if isinstance(raw, (list, tuple, UserList)) else None
        self._tail = 0

    def _prefix(self):
        if self._levels is None:
            match = _BOOK_PREFIX.match(self._raw) if isinstance(self._raw, str) else None
            if match:
                self._tail = match.end()
                comma = self._raw.rfind(',', 0, self._tail)
                self._levels = decode_market_json(self._raw[:comma] + ']')
            else:
                self._levels = decode_market_json(self._raw)
        return self._levels

    @cached_property
    def data(self):
        levels = self._prefix()
        if self._tail:
            levels.extend(decode_market_json('[' + self._raw[self._tail:]))
            self._tail = 0
        return levels

    def __copy__(self):
        return self.data.copy()

    def _extend_to(self, stop):
        start = cursor = self._tail
        for _ in range(stop - len(self._levels)):
            match = _NEXT_LEVEL.match(self._raw, cursor)
            if match is None:
                # End of the book or an uncommon layout: decode the remaining JSON.
                return self.data
            end = match.end(1)
            cursor = match.end()
        self._levels.extend(decode_market_json('[' + self._raw[start:end] + ']'))
        self._tail = cursor
        return self._levels

    def __getitem__(self, index):
        levels = self._prefix()
        if self._tail:
            if isinstance(index, slice):
                if ((index.step is None or index.step > 0) and (index.start or 0) >= 0
                        and index.stop is not None and index.stop >= 0):
                    if index.stop > len(levels) and (index.start or 0) < index.stop:
                        levels = self._extend_to(index.stop)
                else:
                    levels = self.data
            elif index >= len(levels):
                levels = self._extend_to(index + 1)
            elif index < 0:
                levels = self.data
        return levels[index]

    def __iter__(self):
        levels = self._prefix()
        yield from levels
        index = len(levels)
        while self._tail or index < len(levels):
            if index == len(levels):
                self._extend_to(index + 1)
            yield levels[index]
            index += 1


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


def get_interval_seconds(directory, data_file='market_rows.jsonl.gz', *,
                         allow_gaps=False, missing=(), declared=None):
    """Infer a fixed whole-second interval from the leading timestamps."""
    with gzip.open(Path(directory)/data_file, 'rt', encoding='utf-8') as source:
        seconds = []
        for line in islice(source, 3 if allow_gaps else 2):
            stamp = datetime.fromisoformat(json.loads(line)['time'].replace('Z', '+00:00'))
            if stamp.tzinfo is None:
                raise ValueError('Market record has no timezone')
            seconds.append(math.floor(stamp.timestamp()))
    valid_declared = type(declared) is int and declared > 0
    if len(seconds) < 2:
        if seconds and valid_declared:
            return declared
        raise ValueError('At least two market rows or one row with interval_seconds are needed')
    steps = []
    for first, second in zip(seconds, seconds[1:]):
        skipped = sum(right - left for left, right in missing if first < left < right <= second)
        step = second - first - skipped
        if step < 1:
            raise ValueError('Backtest requires fixed whole-second OHLCV intervals')
        steps.append(step)
    interval = math.gcd(*steps)
    if not missing and valid_declared and interval % declared == 0:
        interval = declared
    return interval


def load_market_manifest(directory, data_file='market_rows.jsonl.gz', manifest_file='fetch_manifest.json',
                         *, allow_gaps=False):
    directory = Path(directory)
    manifest = json.loads((directory/manifest_file).read_text(encoding='utf-8'))
    return validate_market_manifest(manifest, directory/data_file, allow_gaps=allow_gaps)


def validate_market_manifest(manifest, raw_path, *, allow_gaps=False):
    missing = manifest.get('missing_intervals_utc_seconds') or ()
    if manifest.get('invalid_rows') or (missing and not allow_gaps):
        raise ValueError('Market data contains missing or invalid rows')
    expected_hash = manifest.get('raw_sha256')
    if expected_hash is None:
        expected_hash = manifest.get('output', {}).get('sha256')
    if not isinstance(expected_hash, str) or not expected_hash:
        raise ValueError('Manifest has no data SHA256')
    if file_hash(raw_path) != expected_hash:
        raise ValueError('Raw data hash does not match the manifest')
    if parse_time(manifest['end_utc_exclusive']) <= parse_time(manifest['start_utc']):
        raise ValueError('Invalid market-data period')
    # Normalize in memory for runner/plot consumers; never rewrite the source manifest.
    return dict(manifest, raw_sha256=expected_hash,
                interval_seconds=get_interval_seconds(
                    raw_path.parent, raw_path.name, allow_gaps=allow_gaps, missing=missing if allow_gaps else (),
                    declared=manifest.get('interval_seconds') if allow_gaps else None))


def iter_market_rows(directory, start, end, data_file='market_rows.jsonl.gz', *, book_decoder=decode_market_json):
    """Decode stored market data without row-level validation."""
    with gzip.open(Path(directory)/data_file, 'rt', encoding='utf-8') as source:
        for line in source:
            row = MarketRow(decode_market_json(line))
            stamp = datetime.fromisoformat(row['time'].replace('Z', '+00:00'))
            timestamp = stamp.timestamp()
            second = math.floor(timestamp)
            if second < start:
                continue
            if second >= end:
                break
            # Replay consumes the stored values like the bot consumes exchange
            # data; only decode the book fields needed by the strategy/model.
            if book_decoder is not None:
                row['bids'] = book_decoder(row['bids_raw'])
                row['asks'] = book_decoder(row['asks_raw'])
            row['timestamp'], row['second'] = timestamp, second
            yield row


def iter_replay_rows(directory, start, end, data_file='market_rows.jsonl.gz'):
    return iter_market_rows(directory, start, end, data_file, book_decoder=_ReplayBook)
