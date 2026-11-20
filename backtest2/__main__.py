"""Shared offline runner: strategy-specific cases come from an experiment module."""
import argparse
import asyncio
import importlib
from pathlib import Path

from .replay import ReplayConfig
from .data import parse_time
from .runner import run_backtest

def main():
    defaults = ReplayConfig()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--experiment', required=True, help='Module exposing build_cases(), e.g. backtests.cfdmm')
    parser.add_argument('--case', action='append', help='Case name; repeat to select multiple cases')
    parser.add_argument('--data', type=Path, required=True, help='Saved market-data directory with manifest')
    parser.add_argument('--data-file', default='market_rows.jsonl.gz', help='Gzip JSONL path, relative to --data or absolute')
    parser.add_argument('--manifest-file', default='fetch_manifest.json', help='Manifest path, relative to --data or absolute')
    parser.add_argument('--output', type=Path, required=True, help='Output directory')
    parser.add_argument('--overwrite', action='store_true',
                        help='Delete the existing output directory before replaying')
    parser.add_argument('--start', help='Inclusive ISO timestamp with timezone')
    parser.add_argument('--end', help='Exclusive ISO timestamp with timezone')
    parser.add_argument('--split-at', action='append', default=[], help='Restart bot/inventory at this ISO timestamp')
    parser.add_argument('--model', choices=('touch', 'through', 'queue_touch'), action='append',
                        help='Fill model; defaults to queue_touch; repeat to compare models')
    parser.add_argument('--latency', type=float, default=defaults.latency)
    parser.add_argument('--cancel-latency', type=float, default=1.)
    parser.add_argument('--decision-offset', type=float, default=defaults.decision_offset)
    parser.add_argument('--warmup-seconds', type=int, default=0)
    parser.add_argument('--no-order-history', action='store_true', help='Disable all-order history recording')
    args = parser.parse_args()
    module = importlib.import_module(args.experiment)
    cases = module.build_cases()
    if args.case:
        unknown = set(args.case) - {case.name for case in cases}
        if unknown:
            parser.error('Unknown cases: ' + ', '.join(sorted(unknown)))
        cases = [case for case in cases if case.name in args.case]
    configs = [ReplayConfig(model=model, latency=args.latency, cancel_latency=args.cancel_latency,
                            decision_offset=args.decision_offset, warmup_seconds=args.warmup_seconds,
                            record_orders=not args.no_order_history)
               for model in (args.model or [defaults.model])]
    result = asyncio.run(run_backtest(args.data, args.output, cases, configs, module.__file__,
                                      parse_time(args.start) if args.start else None,
                                      parse_time(args.end) if args.end else None,
                                      [parse_time(t) for t in args.split_at], overwrite=args.overwrite,
                                      data_file=args.data_file, manifest_file=args.manifest_file))
    print(f'Saved {len(result["results"])} results to {args.output}')


if __name__ == '__main__':
    main()
