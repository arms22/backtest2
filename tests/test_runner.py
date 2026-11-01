from pathlib import Path
import tempfile
import unittest

from backtest2.runner import prepare_output


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
