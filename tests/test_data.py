import json
import math
from pathlib import Path
import tempfile
import unittest

from backtest2.data import decode_market_json, load_market_manifest
from helpers import save_market


class MarketJsonTests(unittest.TestCase):
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
