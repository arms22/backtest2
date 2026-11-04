import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from backtest2 import __main__ as cli


class ModelSelectionTests(unittest.TestCase):
    def test_default_and_explicit_models(self):
        experiment = SimpleNamespace(build_cases=lambda: [], __file__=__file__)
        for options, expected in (
                ([], ['queue_touch']),
                (['--model', 'through'], ['through']),
                (['--model', 'touch', '--model', 'queue_touch'], ['touch', 'queue_touch'])):
            with self.subTest(options=options):
                argv = ['backtest2', '--experiment', 'example', '--data', 'data',
                        '--output', 'output', *options]
                with patch('sys.argv', argv), \
                        patch.object(cli, 'run_backtest',
                              new_callable=AsyncMock, return_value={'results': []}) as run, \
                        patch('builtins.print'), \
                        patch.object(cli.importlib, 'import_module', return_value=experiment):
                    cli.main()
                configs = run.call_args.args[3]
                self.assertEqual([config.model for config in configs], expected)
