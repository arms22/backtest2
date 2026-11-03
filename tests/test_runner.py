import json
from pathlib import Path
import tempfile
import unittest

from backtest2.data import file_hash
from backtest2.audit import audit
from backtest2.plot import load_pnl_series
from backtest2.runner import BotCase, prepare_output, run_backtest, source_hashes
from backtest2.replay import ReplayConfig
from helpers import Adapter, PassiveBot, save_market


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_input_paths_are_reused_by_audit_and_plot(self):
        with tempfile.TemporaryDirectory() as directory:
            data, output = Path(directory)/'data', Path(directory)/'output'
            data.mkdir()
            save_market(data, range(2000, 2004))
            (data/'market_rows.jsonl.gz').rename(data/'quotes.gz')
            (data/'fetch_manifest.json').rename(data/'source.json')
            manifest_path = data/'source.json'
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            manifest['output'] = dict(file='quotes.gz', sha256=manifest.pop('raw_sha256'))
            del manifest['missing_intervals_utc_seconds'], manifest['invalid_rows']
            manifest_text = json.dumps(manifest)
            manifest_path.write_text(manifest_text, encoding='utf-8')
            case = BotCase('custom', PassiveBot, Adapter)
            bundle = await run_backtest(data, output, [case], [ReplayConfig()],
                                        __file__, progress=lambda text: None,
                                        data_file='quotes.gz', manifest_file='source.json')
            self.assertEqual(bundle['input_data_file'], 'quotes.gz')
            self.assertEqual(bundle['input_manifest_file'], 'source.json')
            self.assertTrue(audit(data, output)['checks'][0]['flat'])
            _, curves = load_pnl_series(data, output)
            self.assertEqual(curves[0]['values'][-1], 0.)
            self.assertEqual(manifest_path.read_text(encoding='utf-8'), manifest_text)

    async def test_segments_output_and_fresh_bots(self):
        bots = []

        def create_bot():
            bot = PassiveBot()
            bots.append(bot)
            return bot

        with tempfile.TemporaryDirectory() as directory:
            data, output = Path(directory)/'data', Path(directory)/'output'
            data.mkdir()
            save_market(data, range(2000, 2004))
            metadata = dict(label='fixture', parameters=dict(interval=3))
            case = BotCase('other_logic', create_bot, Adapter, metadata=metadata)
            result = await run_backtest(data, output, [case], [ReplayConfig()],
                                        __file__, split_at=[2002], progress=lambda text: None)
            self.assertEqual(len(result['results']), 2)
            self.assertEqual(len(bots), 2)
            self.assertIsNot(bots[0], bots[1])
            self.assertTrue((output/'results.json').exists())
            self.assertTrue((output/'REPORT.md').exists())
            saved = json.loads((output/'results.json').read_text(encoding='utf-8'))
            self.assertEqual([r['bot_metadata'] for r in saved['results']], [metadata, metadata])
            self.assertTrue(all(r['gross_pnl'] == 0 and r['turnover'] == 0
                                for r in result['results']))

    async def test_bad_data_never_creates_completion_result(self):
        with tempfile.TemporaryDirectory() as directory:
            data, output = Path(directory)/'data', Path(directory)/'output'
            data.mkdir()
            save_market(data, [2000, 2002, 2003])
            manifest_path = data/'fetch_manifest.json'
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            del manifest['missing_intervals_utc_seconds'], manifest['invalid_rows']
            manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'Missing, duplicate'):
                await run_backtest(data, output, [BotCase('other', PassiveBot, Adapter)],
                                   [ReplayConfig()], __file__, progress=lambda text: None)
            self.assertFalse((output/'results.json').exists())


class SourceTests(unittest.TestCase):
    def test_source_provenance_accepts_external_repository(self):
        source = Path(__file__).resolve()
        with tempfile.TemporaryDirectory() as directory:
            external = Path(directory)/'external_repo'/'strategy.py'
            external.parent.mkdir()
            external.write_text('# External strategy fixture\n', encoding='utf-8')
            hashes = source_hashes([BotCase('test', PassiveBot, Adapter, code_files=(external,))], source)
            self.assertTrue(any(path.endswith('external_repo/strategy.py') for path in hashes))
            self.assertIn(file_hash(external), hashes.values())
        self.assertTrue(any(path.endswith('tests/test_runner.py') for path in hashes))
        self.assertTrue(any(path.endswith('backtest2/replay.py') for path in hashes))


class OutputTests(unittest.TestCase):
    def test_existing_results_require_explicit_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'output'
            output.mkdir()
            result = output/'results.json'
            result.write_text('previous', encoding='utf-8')
            with self.assertRaisesRegex(FileExistsError, '--overwrite'):
                prepare_output(output)
            self.assertEqual(result.read_text(encoding='utf-8'), 'previous')
            (output/'plots').mkdir()
            (output/'plots'/'old.svg').write_text('old', encoding='utf-8')
            prepare_output(output, overwrite=True)
            self.assertTrue(output.is_dir())
            self.assertEqual(list(output.iterdir()), [])

    def test_overwrite_preserves_protected_data_and_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'output'
            output.mkdir()
            protected = output/'source.py'
            protected.write_text('keep', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'contains input data'):
                prepare_output(output, overwrite=True, protected_paths=[protected])
            self.assertEqual(protected.read_text(encoding='utf-8'), 'keep')

    def test_overwrite_also_accepts_new_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'new'/'output'
            prepare_output(output, overwrite=True)
            self.assertTrue(output.is_dir())


if __name__ == '__main__':
    unittest.main()
