import json
import asyncio
import gzip
from types import ModuleType
from pathlib import Path
import tempfile
import unittest

from backtest2.data import file_hash, iso
from backtest2.audit import audit
from backtest2.plot import load_pnl_series
from backtest2.runner import BotCase, prepare_output, run_backtest, source_hashes
from backtest2.replay import ReplayConfig, wakeup
from helpers import Adapter, PassiveBot, save_market


class MultiInputTests(unittest.IsolatedAsyncioTestCase):
    def save_input(self, directory, seconds, end, missing=()):
        directory.mkdir()
        save_market(directory, seconds)
        path = directory/'fetch_manifest.json'
        manifest = json.loads(path.read_text(encoding='utf-8'))
        manifest.update(start_utc=iso(seconds[0]), end_utc_exclusive=iso(end),
                        missing_intervals_utc_seconds=missing)
        path.write_text(json.dumps(manifest), encoding='utf-8')
        return directory/'market_rows.jsonl.gz'

    async def run_case(self, data, output):
        class Bot:
            async def run(self, strategy):
                await wakeup()
                strategy.engine.submit_order('BUY', .003, 100.)
                while True:
                    await wakeup()

        return await run_backtest(data, output, [BotCase('trading', Bot, Adapter)],
                                  [ReplayConfig(latency=0.)], __file__, progress=lambda _: None)

    async def test_overlapping_files_replay_like_one_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            whole = self.save_input(root/'whole', list(range(2000, 2007)), 2007)
            first = self.save_input(root/'first', [2000, 2001, 2002, 2003], 2004)
            second = self.save_input(root/'second', [2002, 2003, 2004, 2005, 2006], 2007)
            # A different value in a skipped row must not change the replay.
            with gzip.open(second, 'rt', encoding='utf-8') as source:
                rows = [json.loads(line) for line in source]
            rows[0]['bids_raw'] = '[[1, 0.1]]'
            with gzip.open(second, 'wt', encoding='utf-8') as target:
                for row in rows:
                    target.write(json.dumps(row)+'\n')
            path = second.parent/'fetch_manifest.json'
            manifest = json.loads(path.read_text(encoding='utf-8'))
            manifest['raw_sha256'] = file_hash(second)
            path.write_text(json.dumps(manifest), encoding='utf-8')
            single = await self.run_case(whole.parent, root/'single')
            output = root/'multi'
            inputs = [second.parent, first.parent]
            multi = await self.run_case(inputs, output)
            self.assertEqual(multi['results'], single['results'])
            self.assertEqual([item['start_utc'] for item in multi['input_files']], [iso(2000), iso(2004)])
            self.assertEqual(multi['gaps'], [])
            self.assertTrue(audit(inputs, output)['checks'][0]['flat'])
            _, curves = load_pnl_series(inputs, output)
            self.assertAlmostEqual(curves[0]['values'][-1], multi['results'][0]['gross_pnl'])

    async def test_single_gap_continues_and_larger_gap_liquidates(self):
        for interval in (1, 60):
            with self.subTest(interval=interval), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                start = 2400
                def at(bar):
                    return start + bar * interval

                first = self.save_input(root/'first', [at(0), at(2), at(3)], at(4),
                                        missing=[[at(1), at(2)]] if interval == 60 else [])
                second = self.save_input(root/'second', [at(6), at(7), at(8)], at(9))
                fetcher_file = second.with_name('query_results.jsonl.gz')
                second.rename(fetcher_file)
                manifest_path = second.parent/'fetch_manifest.json'
                manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
                manifest['output'] = dict(file=fetcher_file.name, sha256=manifest.pop('raw_sha256'))
                manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
                inputs, output = [first.parent, second.parent], root/'output'
                bundle = await self.run_case(inputs, output)
                self.assertEqual([Path(item['path']).name for item in bundle['input_files']],
                                 [first.name, fetcher_file.name])
                self.assertEqual([(gap['missing_bars'], gap['action']) for gap in bundle['gaps']],
                                 [(1, 'continue'), (2, 'restart')])
                self.assertEqual(bundle['segments'], [
                    dict(start_utc=iso(at(0)), end_utc_exclusive=iso(at(4))),
                    dict(start_utc=iso(at(6)), end_utc_exclusive=iso(at(9)))])
                first_result, second_result = bundle['results']
                self.assertAlmostEqual(first_result['terminal_position'], .003)
                self.assertAlmostEqual(second_result['terminal_position'], .003)
                with gzip.open(output/'segment_001_trading_queue_touch_0s_fills.jsonl.gz',
                               'rt', encoding='utf-8') as source:
                    fills = [json.loads(line) for line in source]
                self.assertEqual([fill['kind'] for fill in fills], ['maker', 'terminal'])
                self.assertAlmostEqual(fills[0]['t'], at(2) + .01)
                self.assertAlmostEqual(fills[-1]['t'], at(3) + .01)
                self.assertEqual(len(audit(inputs, output)['checks']), 2)
                _, curves = load_pnl_series(inputs, output)
                self.assertAlmostEqual(curves[0]['values'][-1], sum(r['gross_pnl'] for r in bundle['results']))


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_runner_scopes_asyncio_patch_to_selected_modules(self):
        module = ModuleType('example_bot')
        module.asyncio = asyncio
        original_gather = asyncio.gather
        observed = []

        class Bot(PassiveBot):
            async def run(self, strategy):
                observed.append(module.asyncio is not asyncio)
                observed.append(asyncio.gather is original_gather)
                await super().run(strategy)

        with tempfile.TemporaryDirectory() as directory:
            data, output = Path(directory)/'data', Path(directory)/'output'
            data.mkdir()
            save_market(data, range(2000, 2004))
            case = BotCase('patched', Bot, Adapter, asyncio_modules=(module,))
            await run_backtest(data, output, [case], [ReplayConfig()], __file__,
                               progress=lambda text: None)
        self.assertEqual(observed, [True, True])
        self.assertIs(module.asyncio, asyncio)

    async def test_minute_and_hour_intervals_use_data_timestamps(self):
        for interval in (60, 3600):
            with self.subTest(interval=interval), tempfile.TemporaryDirectory() as directory:
                data, output = Path(directory)/'data', Path(directory)/'output'
                data.mkdir()
                save_market(data, [0, interval, interval * 2, interval * 3])
                manifest_path = data/'fetch_manifest.json'
                manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
                manifest['start_utc'] = '1970-01-01T00:00:00Z'
                manifest['end_utc_exclusive'] = '1970-01-01T00:04:00Z' if interval == 60 else '1970-01-01T04:00:00Z'
                manifest['interval_seconds'] = 59
                manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
                bundle = await run_backtest(data, output, [BotCase('other', PassiveBot, Adapter)],
                                            [ReplayConfig()], __file__, split_at=[interval * 2],
                                            progress=lambda text: None)
                self.assertEqual(bundle['input_manifest']['interval_seconds'], interval)
                self.assertEqual(len(bundle['results']), 2)
                _, curves = load_pnl_series(data, output)
                self.assertEqual(curves[0]['values'][-1], 0.)

    async def test_saved_custom_input_paths_are_reused_by_audit_and_plot(self):
        with tempfile.TemporaryDirectory() as directory:
            data, output = Path(directory)/'data', Path(directory)/'output'
            data.mkdir()
            save_market(data, range(2000, 2004))
            case = BotCase('custom', PassiveBot, Adapter)
            bundle = await run_backtest(data, output, [case], [ReplayConfig()],
                                        __file__, progress=lambda text: None)
            (data/'market_rows.jsonl.gz').rename(data/'quotes.gz')
            (data/'fetch_manifest.json').rename(data/'source.json')
            manifest_path = data/'source.json'
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            manifest['output'] = dict(file='quotes.gz', sha256=manifest.pop('raw_sha256'))
            del manifest['missing_intervals_utc_seconds'], manifest['invalid_rows']
            manifest_text = json.dumps(manifest)
            manifest_path.write_text(manifest_text, encoding='utf-8')
            bundle.update(input_data_file='quotes.gz', input_manifest_file='source.json')
            (output/'results.json').write_text(json.dumps(bundle), encoding='utf-8')
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
            for row in saved['results']:
                self.assertEqual(row['activity']['fast_seconds'], 60)
                self.assertEqual(row['activity']['normal_quantity'], 0)
                self.assertEqual(set(row['position_activity']), {'BUY', 'SELL'})
            self.assertEqual([r['bot_metadata'] for r in saved['results']], [metadata, metadata])
            self.assertTrue(all(r['gross_pnl'] == 0 and r['turnover'] == 0
                                for r in result['results']))

    async def test_missing_rows_are_not_validated_during_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            data, output = Path(directory)/'data', Path(directory)/'output'
            data.mkdir()
            save_market(data, [2000, 2001, 2003])
            manifest_path = data/'fetch_manifest.json'
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            del manifest['missing_intervals_utc_seconds'], manifest['invalid_rows']
            manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
            await run_backtest(data, output, [BotCase('other', PassiveBot, Adapter)],
                               [ReplayConfig()], __file__, progress=lambda text: None)
            self.assertTrue((output/'results.json').exists())


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
