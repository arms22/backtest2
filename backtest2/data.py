"""Read and validate saved single-product market snapshots; no network access."""
import gzip
import hashlib
import json
import math
import re
from collections import UserList
from datetime import datetime, timezone
from functools import cached_property
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


def get_interval_seconds(directory, data_file='market_rows.jsonl.gz'):
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
                interval_seconds=get_interval_seconds(directory, data_file))


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
            row['bids'] = book_decoder(row['bids_raw'])
            row['asks'] = book_decoder(row['asks_raw'])
            row['timestamp'], row['second'] = timestamp, second
            yield row


def iter_replay_rows(directory, start, end, data_file='market_rows.jsonl.gz'):
    return iter_market_rows(directory, start, end, data_file, book_decoder=_ReplayBook)
