import gzip
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from backtest2.data import file_hash, iso
from backtest2.plot import load_pnl_series, plot_pnl


class PnlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.start = 1700000000

    def save(self, split=False):
        start, end = self.start, self.start + (4 if split else 2)
        raw = self.directory/'market_rows.jsonl.gz'
        with gzip.open(raw, 'wt', encoding='utf-8') as stream:
            for second in range(start, end):
                row = dict(time=iso(second+.01), bid=100, ask=110,
                           bids_raw=json.dumps([[100, .001], [90, .1]]),
                           asks_raw=json.dumps([[110, .001], [120, .1]]),
                           open=105, high=105, low=105, close=105,
                           buy_volume=0, sell_volume=0, buy_count=0, sell_count=0,
                           trades=0, delay=0)
                stream.write(json.dumps(row)+'\n')
        manifest = dict(raw_sha256=file_hash(raw), start_utc=iso(start),
                        end_utc_exclusive=iso(end),
                        missing_intervals_utc_seconds=[], invalid_rows=[])
        (self.directory/'fetch_manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
        results = []
        for index, second in enumerate(range(start, end, 2), 1):
            for case, sides, prices in [('long', ['BUY', 'SELL'], [100, 110]),
                                        ('short', ['SELL', 'BUY'], [110, 100])]:
                name, segment = case+'_through_1s', f'segment_{index:03}'
                results.append(dict(name=name, segment=segment, case=case,
                                    config=dict(model='through'), gross_pnl=.02))
                with gzip.open(self.directory/f'{segment}_{name}_fills.jsonl.gz',
                               'wt', encoding='utf-8') as stream:
                    for offset in range(2):
                        stream.write(json.dumps(dict(t=second+offset+.01,
                                                     side=sides[offset], size=.002,
                                                     price=prices[offset]))+'\n')
        bundle = dict(input_raw_sha256=manifest['raw_sha256'], input_manifest=manifest,
                      start_utc=iso(start), end_utc_exclusive=iso(end),
                      split_at=[iso(start+2)] if split else [], results=results)
        self.write_bundle(bundle)
        return bundle

    def write_bundle(self, bundle):
        (self.directory/'results.json').write_text(json.dumps(bundle), encoding='utf-8')

    def test_long_and_short_consume_book_depth(self):
        self.save()
        _, curves = load_pnl_series(self.directory, self.directory)
        for curve in curves:
            self.assertAlmostEqual(curve['values'][1], -.01)
            self.assertAlmostEqual(curve['values'][-1], .02)

    def test_independent_segments_accumulate_and_filter(self):
        self.save(split=True)
        _, curves = load_pnl_series(self.directory, self.directory, cases=['short'])
        self.assertEqual(len(curves), 1)
        self.assertAlmostEqual(curves[0]['values'][4], .01)
        self.assertAlmostEqual(curves[0]['values'][-1], .04)

    def test_rejects_inconsistent_final_pnl(self):
        bundle = self.save()
        bundle['results'][0]['gross_pnl'] = 1
        self.write_bundle(bundle)
        with self.assertRaisesRegex(ValueError, 'Segment P&L'):
            load_pnl_series(self.directory, self.directory)

    def test_rejects_different_input(self):
        bundle = self.save()
        bundle['input_raw_sha256'] = 'different'
        self.write_bundle(bundle)
        with self.assertRaisesRegex(ValueError, 'Market data differs'):
            load_pnl_series(self.directory, self.directory)

    @unittest.skipUnless(importlib.util.find_spec('matplotlib'), 'matplotlib is optional')
    def test_plot_uses_default_or_custom_title(self):
        bundle = self.save()
        manifest = bundle['input_manifest']
        manifest['output'] = {'sha256': manifest.pop('raw_sha256')}
        (self.directory/'fetch_manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
        self.write_bundle(bundle)
        for options, expected in (({}, 'Cumulative P&amp;L'),
                                  ({'title': 'Custom experiment'}, 'Custom experiment')):
            with self.subTest(options=options):
                output = plot_pnl(self.directory, self.directory, **options)
                self.assertIn(expected, output.read_text(encoding='utf-8'))


if __name__ == '__main__':
    unittest.main()
