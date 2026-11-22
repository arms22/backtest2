import json
import math
from pathlib import Path
import tempfile
import unittest

from backtest2.data import _ReplayBook, decode_market_json, load_market_manifest
from helpers import save_market


class MarketJsonTests(unittest.TestCase):
    def test_replay_book_expands_only_to_requested_depth(self):
        levels = [[i, .1] for i in range(200)]
        book = _ReplayBook(json.dumps(levels, indent=2))
        self.assertEqual(book[20], levels[20])
        self.assertEqual(len(book._levels), 21)
        self.assertEqual(book[:42], levels[:42])
        self.assertEqual(len(book._levels), 42)
        self.assertEqual(book[21], levels[21])
        self.assertEqual(len(book._levels), 42)
        for index, level in enumerate(book):
            self.assertEqual(level, levels[index])
            if index == 42:
                break
        self.assertEqual(len(book._levels), 43)
        self.assertEqual(list(iter(book)), levels)

    def test_replay_book_decodes_prefix_then_tail_once(self):
        levels = [[i, i / 100] for i in range(25)]
        book = _ReplayBook(json.dumps(levels, indent=2))
        self.assertEqual(book[:20], levels[:20])
        self.assertEqual(len(book._levels), 20)
        first = book[0]
        it = iter(book)
        self.assertIs(next(it), first)
        self.assertEqual(book[21], levels[21])
        self.assertEqual(list(it), levels[1:])
        self.assertIs(book[0], first)
        self.assertEqual(book[::-1], levels[::-1])

    def test_replay_book_matches_short_and_full_sequences(self):
        for length in (0, 1, 19, 20, 21, 200):
            levels = [[i, i + .25] for i in range(length)]
            raw = json.dumps(levels)
            self.assertEqual(list(iter(_ReplayBook(raw))), levels)
            self.assertEqual(len(_ReplayBook(raw)), length)
            for key in (slice(None, 0), slice(4, 12, 2), slice(-5, None), slice(None, None, -1)):
                self.assertEqual(_ReplayBook(raw)[key], levels[key])
            if levels:
                self.assertEqual(_ReplayBook(raw)[-1], levels[-1])
            with self.assertRaises(IndexError):
                _ReplayBook(raw)[length]

    def test_preserves_numeric_types_and_legacy_values(self):
        text = '[18446744073709551617, 1, 1.0, -0.0, 1.2345678901234567]'
        decoded = decode_market_json(text)
        self.assertEqual(decoded, json.loads(text))
        self.assertEqual([type(value) for value in decoded], [int, int, float, float, float])
        self.assertEqual(math.copysign(1, decoded[3]), -1)
        self.assertTrue(math.isnan(decode_market_json('[NaN]')[0]))
        with self.assertRaises(json.JSONDecodeError):
            decode_market_json('[invalid]')


class ManifestTests(unittest.TestCase):
    def test_hash_and_quality_flags_are_still_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory)
            save_market(data, range(2000, 2004))
            path = data/'fetch_manifest.json'
            original = json.loads(path.read_text(encoding='utf-8'))
            cases = [
                (dict(raw_sha256='wrong', output=dict(sha256=original['raw_sha256'])),
                 'hash does not match'),
                (dict(raw_sha256=None, output=dict(sha256='wrong')), 'hash does not match'),
                (dict(raw_sha256=None), 'no data SHA256'),
                (dict(missing_intervals_utc_seconds=[[2001, 2002]]), 'missing or invalid'),
                (dict(invalid_rows=[[2001, 'Empty book']]), 'missing or invalid'),
            ]
            for changes, message in cases:
                with self.subTest(changes=changes):
                    path.write_text(json.dumps(dict(original, **changes)), encoding='utf-8')
                    with self.assertRaisesRegex(ValueError, message):
                        load_market_manifest(data)
