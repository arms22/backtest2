"""Run bot-defined cases through the common offline replay engine."""
import asyncio
import bisect
import csv
import gzip
import json
import math
import os
import re
import shutil
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .analysis import (match_fills_fifo, summarize_matches, summarize_orders,
                       summarize_activity, summarize_position_activity)
from .replay import ReplayEngine
from .asyncio_support import sequential_asyncio
from .data import file_hash, iso, iter_replay_rows as iter_market_rows, load_market_manifest, parse_time

@dataclass(frozen=True)
class BotCase:
    name: str
    create_bot: Callable
    strategy_factory: Callable
    clock_context: Callable = nullcontext
    code_files: tuple = ()
    metadata: dict = field(default_factory=dict)
    asyncio_modules: tuple = ()

    def __post_init__(self):
        if not re.fullmatch(r'[A-Za-z0-9_-]+', self.name):
            raise ValueError('Case names must contain only letters, digits, underscores or hyphens')


def source_hashes(cases, experiment_file):
    files = set(Path(__file__).parent.glob('*.py')) | {Path(experiment_file)}
    for case in cases:
        files.update(Path(p) for p in case.code_files)
    files = sorted({p.resolve() for p in files})
    root = Path(os.path.commonpath([p.parent for p in files]))
    return {p.relative_to(root).as_posix(): file_hash(p) for p in files}



def markout_horizons(interval_seconds):
    """Choose useful markout horizons from the inferred market-data interval."""
    unit = 1 if interval_seconds < 60 else 60 if interval_seconds < 3600 else 3600
    return sorted({interval_seconds, *(value * unit for value in (1, 5, 30, 60))})


def fill_markout_rows(fills, observations, interval_seconds):
    horizons = markout_horizons(interval_seconds)
    records = []
    for fill in fills:
        if fill['kind'] == 'terminal':
            continue
        markouts = {}
        for horizon in horizons:
            target_time = fill['t'] + horizon
            index = bisect.bisect_left(observations, (target_time, -math.inf))
            if index == len(observations):
                markouts[str(horizon)] = dict(
                    target_time=target_time, observed_time=None,
                    actual_horizon_seconds=None, mid_price=None,
                    yen_per_btc=None, markout_yen=None, status='censored')
                continue
            observed_time, future = observations[index]
            sign = 1 if fill['side'] == 'BUY' else -1
            change = sign * (future - fill['price'])
            markouts[str(horizon)] = dict(
                target_time=target_time, observed_time=observed_time,
                actual_horizon_seconds=observed_time - fill['t'],
                mid_price=future, yen_per_btc=change,
                markout_yen=change * fill['size'], status='observed')
        records.append(dict(
            fill_id=fill['fill_id'], t=fill['t'], side=fill['side'],
            kind=fill['kind'], price=fill['price'], size=fill['size'],
            order=fill['order'], realized_pnl=fill['realized_pnl'],
            markouts=markouts))
    return records


def summarize_markouts(records):
    result = {}
    horizons = sorted({int(horizon) for record in records
                       for horizon in record['markouts']})
    for horizon in horizons:
        changes, volume = [], 0.
        eligible_count = censored_count = 0
        eligible_volume = censored_volume = 0.
        for record in records:
            fill_markout = record['markouts'][str(horizon)]
            if fill_markout['status'] == 'censored':
                censored_count += 1
                censored_volume += record['size']
                continue
            eligible_count += 1
            eligible_volume += record['size']
            changes.append(fill_markout['markout_yen'])
            volume += record['size']
        markout_yen = math.fsum(changes)
        result[str(horizon)] = dict(
            count=len(changes), volume=volume,
            yen_per_btc=markout_yen/volume if volume else None,
            markout_yen=markout_yen,
            eligible_count=eligible_count, observed_count=len(changes),
            censored_count=censored_count,
            eligible_volume=eligible_volume, observed_volume=volume,
            censored_volume=censored_volume,
        )
    return result


def markouts_by_side_kind(records):
    groups = {}
    for record in records:
        key = f"{record['side']}/{record['kind']}"
        groups.setdefault(key, []).append(record)
    return {key: summarize_markouts(group)
            for key, group in sorted(groups.items())}


def prepare_output(output, overwrite=False, protected_paths=()):
    """Replace an explicitly selected result directory, protecting inputs and sources."""
    output = Path(output)
    if output.exists() and not overwrite:
        raise FileExistsError(f'Output directory already exists: {output}. '
                              'Use --overwrite to replace it, or choose another --output.')
    if overwrite and output.exists():
        target = output.resolve()
        if output.is_symlink() or (hasattr(output, 'is_junction') and output.is_junction()):
            raise ValueError('Cannot overwrite a linked output directory')
        if target == Path(target.anchor) or any(Path(p).resolve().is_relative_to(target)
                                                for p in protected_paths):
            raise ValueError('Output directory contains input data, working directory or source files')
        shutil.rmtree(target)
    output.mkdir(parents=True, exist_ok=False)


def _validate_run(manifest, start, end, split_at, cases, configs):
    """Resolve the period and reject inconsistent replay selections."""
    data_start = parse_time(manifest['start_utc'])
    data_end = parse_time(manifest['end_utc_exclusive'])
    start = data_start if start is None else start
    end = data_end if end is None else end
    interval_seconds = manifest['interval_seconds']
    if any((value - data_start) % interval_seconds for value in (start, end, *split_at)):
        raise ValueError('Period and split boundaries must align to market intervals')
    if not data_start <= start < end <= data_end:
        raise ValueError('Requested period lies outside the saved market data')
    if list(split_at) != sorted(set(split_at)) or any(not start < t < end for t in split_at):
        raise ValueError('Split boundaries must be unique, ordered and inside the requested period')
    if not cases or not configs or len({c.name for c in cases}) != len(cases):
        raise ValueError('Select at least one case/model and use unique case names')
    if len(set(configs)) != len(configs):
        raise ValueError('Duplicate execution configurations')
    config_names = [f'{c.model}_{c.latency:g}s' for c in configs]
    if len(set(config_names)) != len(config_names):
        raise ValueError('Configurations need distinct model/latency labels; run other sensitivities separately')
    return start, end


async def _finish_segment(engines, output, completed_runs):
    for engine, case in engines:
        result = await engine.finish()
        result['orders_file'] = None
        if engine.config.record_orders:
            filename = f'{engine.segment}_{engine.name}_orders.jsonl.gz'
            with gzip.open(output/filename, 'wt', encoding='utf-8') as target:
                for record in engine.order_history.values():
                    target.write(json.dumps(record, separators=(',', ':'), allow_nan=False) + '\n')
            result['orders_file'] = filename
            result['order_summary'] = summarize_orders(engine.order_history.values())
        result['case'] = case.name
        result['bot_metadata'] = case.metadata
        matches, open_lots = match_fills_fifo(engine.fills)
        result['match_slice_count'] = len(matches)
        result['match_realized_pnl'] = math.fsum(
            match['gross_realized_pnl'] for match in matches)
        result['open_lot_count_at_cutoff'] = len(open_lots)
        result['open_quantity_at_cutoff'] = math.fsum(
            lot['size'] for lot in open_lots)
        assert math.isclose(result['match_realized_pnl'], result['gross_pnl'], abs_tol=1e-5)
        assert math.isclose(math.fsum(match['turnover'] for match in matches),
                            result['turnover'], abs_tol=1e-4)
        assert math.isclose(result['open_quantity_at_cutoff'],
                            abs(result['terminal_position']), abs_tol=1e-8)
        result['fill_match_summary'] = summarize_matches(matches)
        result['activity'] = summarize_activity(engine.fills, matches)
        result['position_activity'] = {
            side: summarize_position_activity(matches, entry_side=side)
            for side in ('BUY', 'SELL')}
        completed_runs.append((result, engine.fills))
        path = output/f'{engine.segment}_{engine.name}_fills.jsonl.gz'
        with gzip.open(path, 'wt', encoding='utf-8') as target:
            for fill in engine.fills:
                target.write(json.dumps(fill, separators=(',', ':')) + '\n')
        for suffix, records in (('matches', matches), ('open_lots', open_lots)):
            derived_path = output/f'{engine.segment}_{engine.name}_{suffix}.jsonl.gz'
            with gzip.open(derived_path, 'wt', encoding='utf-8') as target:
                for record in records:
                    target.write(json.dumps(record, separators=(',', ':')) + '\n')


def _write_reports(output, results, start, end):
    """Write human-readable summaries after replay and analysis complete."""
    with (output/'summary.csv').open('w', newline='', encoding='utf-8-sig') as target:
        fields = ['segment', 'name', 'gross_pnl', 'turnover', 'pnl_per_million',
                  'max_drawdown_liquidation_mtm', 'max_abs_position', 'terminal_position',
                  'buy_orders', 'buy_filled_orders', 'buy_fill_rate',
                  'sell_orders', 'sell_filled_orders', 'sell_fill_rate']
        writer = csv.DictWriter(target, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    with (output/'match_summary.csv').open('w', newline='', encoding='utf-8-sig') as target:
        fields = ['segment', 'name', 'group_by', 'group_key', 'match_count',
                  'quantity', 'turnover', 'gross_realized_pnl', 'win_rate',
                  'average_pnl_per_match', 'pnl_per_million_turnover']
        writer = csv.DictWriter(target, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for result in results:
            for row in result['fill_match_summary']:
                writer.writerow(dict(segment=result['segment'], name=result['name'], **row))
    lines = ['# Backtest results\n',
             f'Period: {iso(start)} <= time < {iso(end)}\n',
             'PnL and drawdown in JPY; turnover includes both sides. Fees are zero; funding, '
             'rollover, actual API waits and subsecond queue dynamics are not modeled.\n',
             'FIFO close slices and grouped realized PnL are in `*_matches.jsonl.gz` and '
             '`match_summary.csv`. `*_open_lots.jsonl.gz` records inventory before terminal '
             'liquidation.\n',
             '| Segment | Case | PnL | Turnover | PnL / 1M turnover | MTM drawdown |\n'
             '| --- | --- | ---: | ---: | ---: | ---: |']
    for result in results:
        rate = f'{result["pnl_per_million"]:,.2f}' if result['pnl_per_million'] is not None else 'N/A'
        lines.append(f'| {result["segment"]} | {result["name"]} | {result["gross_pnl"]:,.2f} | '
                     f'{result["turnover"]:,.2f} | {rate} | {result["max_drawdown_liquidation_mtm"]:,.2f} |')
    lines.extend(['', 'Order fill rate counts each order with any fill once, including partial fills. '
                  'Submitted orders include rejections and cancellations; terminal liquidation is excluded. '
                  'No submitted orders means N/A.', '',
                  '| Segment | Case | Side | Orders | Filled orders | Fill rate |',
                  '| --- | --- | --- | ---: | ---: | ---: |'])
    for result in results:
        for side in ('buy', 'sell'):
            value = result[side + '_fill_rate']
            rate = f'{value:.2%}' if value is not None else 'N/A'
            lines.append(f'| {result["segment"]} | {result["name"]} | {side.upper()} | '
                         f'{result[side + "_orders"]} | {result[side + "_filled_orders"]} | {rate} |')
    (output/'REPORT.md').write_text('\n'.join(lines)+'\n', encoding='utf-8')


async def run_backtest(data, output, cases, configs, experiment_file, start=None, end=None,
                       split_at=(), progress=print, overwrite=False,
                       data_file='market_rows.jsonl.gz', manifest_file='fetch_manifest.json'):
    """Fresh bot/inventory per segment; behavior and parameters come from each case."""
    data, output = Path(data), Path(output)
    manifest = load_market_manifest(data, data_file, manifest_file)
    start, end = _validate_run(manifest, start, end, split_at, cases, configs)
    interval_seconds = manifest['interval_seconds']
    hashes = source_hashes(cases, experiment_file)
    protected = [data, data/data_file, data/manifest_file, Path.cwd(), Path(__file__).parent, experiment_file]
    protected.extend(p for case in cases for p in case.code_files)
    prepare_output(output, overwrite, protected)
    boundaries = [start, *split_at, end]
    engines, midpoints, segment_index = [], [], -1
    completed_runs = []

    with ExitStack() as contexts:
        contexts.enter_context(sequential_asyncio(
            module for case in cases for module in case.asyncio_modules))
        # Only external clocks are rebound. Indicator and order rules stay in the bot.
        for case in cases:
            contexts.enter_context(case.clock_context())
        try:
            for index, row in enumerate(iter_market_rows(data, start, end, data_file), 1):
                second = row['second']
                midpoints.append((row['timestamp'], (row['bid'] + row['ask']) / 2))
                while segment_index < 0 or second >= boundaries[segment_index + 1]:
                    if engines:
                        await _finish_segment(engines, output, completed_runs)
                    segment_index += 1
                    engines = []
                    for case in cases:
                        for config in configs:
                            name = f'{case.name}_{config.model}_{config.latency:g}s'
                            engine = ReplayEngine(case.create_bot(), config, boundaries[segment_index],
                                                  boundaries[segment_index + 1],
                                                  f'segment_{segment_index + 1:03}', name,
                                                  case.strategy_factory, interval_seconds)
                            engines.append((engine, case))
                            await engine.launch()
                for engine, case in engines:
                    await engine.tick(row)
                if (second + interval_seconds - start) % 3600 == 0:
                    progress(f'Replayed {iso(second+interval_seconds)} ({len(engines)} cases/models)')
                if index % 256 == 0:
                    # Let the caller cancel a long run without yielding on every tick.
                    await asyncio.sleep(0)
            await _finish_segment(engines, output, completed_runs)
        finally:
            await asyncio.gather(*(engine.stop() for engine, case in engines), return_exceptions=True)

    midpoints.sort()
    for result, fills in completed_runs:
        records = fill_markout_rows(fills, midpoints, interval_seconds)
        result['fill_markouts'] = summarize_markouts(records)
        result['fill_markouts_by_side_kind'] = markouts_by_side_kind(records)
        filename = f"{result['segment']}_{result['name']}_fill_markouts.jsonl.gz"
        result['fill_markouts_file'] = filename
        with gzip.open(output/filename, 'wt', encoding='utf-8') as target:
            for record in records:
                target.write(json.dumps(record, separators=(',', ':')) + '\n')
    if source_hashes(cases, experiment_file) != hashes:
        raise ValueError('Source files changed during the replay; no completion result written')
    results = [result for result, fills in completed_runs]
    _write_reports(output, results, start, end)
    # Written last: its presence marks a successfully completed run.
    bundle = dict(input_manifest=manifest, input_raw_sha256=manifest['raw_sha256'],
                  input_data_file=str(data_file), input_manifest_file=str(manifest_file),
                  start_utc=iso(start), end_utc_exclusive=iso(end), split_at=[iso(t) for t in split_at],
                  code_sha256=hashes, results=results)
    (output/'results.json').write_text(json.dumps(bundle, indent=2), encoding='utf-8')
    return bundle
