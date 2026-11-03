import json
from pathlib import Path
import tempfile
import unittest

from backtest2.data import load_market_manifest
from helpers import save_market


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
