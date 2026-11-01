"""Run bot-defined cases through the common offline replay engine."""
import asyncio
import csv
import gzip
import json
import math
import os
import re
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .replay import ReplayEngine
from .data import file_hash, iso, iter_market_rows, load_market_manifest, parse_time

@dataclass(frozen=True)
class BotCase:
    name: str
    create_bot: Callable
    strategy_factory: Callable
    clock_context: Callable = nullcontext
    code_files: tuple = ()
    metadata: dict = field(default_factory=dict)

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



def markouts(fills, midpoints):
    result = {}
    for horizon in (1, 5, 30, 60):
        changes, volume = [], 0.
        for fill in fills:
            future = midpoints.get(math.floor(fill['t']) + horizon)
            if future is None or fill['kind'] == 'terminal':
                continue
            sign = 1 if fill['side'] == 'BUY' else -1
            changes.append(sign * (future - fill['price']) * fill['size'])
            volume += fill['size']
        result[str(horizon)] = dict(count=len(changes), volume=volume,
                                    yen_per_btc=math.fsum(changes)/volume if volume else None)
    return result


async def run_backtest(data, output, cases, configs, experiment_file, start=None, end=None,
                       split_at=(), progress=print):
    """Fresh bot/inventory per segment; behavior and parameters come from each case."""
    data, output = Path(data), Path(output)
    manifest = load_market_manifest(data)
    data_start = parse_time(manifest['start_utc'])
    data_end = parse_time(manifest['end_utc_exclusive'])
    start = data_start if start is None else start
    end = data_end if end is None else end
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
    hashes = source_hashes(cases, experiment_file)
    output.mkdir(parents=True, exist_ok=False)
    boundaries = [start, *split_at, end]
    results, engines, midpoints, segment_index = [], [], {}, -1

    async def finish_segment():
        for engine, case in engines:
            result = await engine.finish()
            result['case'] = case.name
            result['bot_metadata'] = case.metadata
            results.append(result)
            path = output/f'{engine.segment}_{engine.name}_fills.jsonl.gz'
            with gzip.open(path, 'wt', encoding='utf-8') as target:
                for fill in engine.fills:
                    target.write(json.dumps(fill, separators=(',', ':')) + '\n')

    with ExitStack() as contexts:
        # Only external clocks are rebound. Indicator and order rules stay in the bot.
        for case in cases:
            contexts.enter_context(case.clock_context())
        try:
            for row in iter_market_rows(data, start, end, manifest['product']):
                second = row['second']
                midpoints[second] = (row['bid'] + row['ask']) / 2
                if segment_index < 0 or second == boundaries[segment_index + 1]:
                    if engines:
                        await finish_segment()
                    segment_index += 1
                    engines = []
                    for case in cases:
                        for config in configs:
                            name = f'{case.name}_{config.model}_{config.latency:g}s'
                            engine = ReplayEngine(case.create_bot(), config, boundaries[segment_index],
                                                  boundaries[segment_index + 1],
                                                  f'segment_{segment_index + 1:03}', manifest['product'], name,
                                                  case.strategy_factory)
                            engines.append((engine, case))
                            await engine.launch()
                for engine, case in engines:
                    await engine.tick(row)
                if (second + 1 - start) % 3600 == 0:
                    progress(f'Replayed {iso(second+1)} ({len(engines)} cases/models)')
            await finish_segment()
        finally:
            await asyncio.gather(*(engine.stop() for engine, case in engines), return_exceptions=True)

    for result in results:
        path = output/f'{result["segment"]}_{result["name"]}_fills.jsonl.gz'
        with gzip.open(path, 'rt', encoding='utf-8') as source:
            result['fill_markouts'] = markouts([json.loads(line) for line in source], midpoints)
    if source_hashes(cases, experiment_file) != hashes:
        raise ValueError('Source files changed during the replay; no completion result written')
    with (output/'summary.csv').open('w', newline='', encoding='utf-8-sig') as target:
        fields = ['segment', 'name', 'gross_pnl', 'turnover', 'pnl_per_million',
                  'max_drawdown_liquidation_mtm', 'max_abs_position', 'terminal_position']
        writer = csv.DictWriter(target, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    lines = ['# Backtest results\n',
             f'Period: {iso(start)} <= time < {iso(end)}\n',
             'PnL and drawdown in JPY; turnover includes both sides. Fees are zero; funding, '
             'rollover, actual API waits and subsecond queue dynamics are not modeled.\n',
             '| Segment | Case | PnL | Turnover | PnL / 1M turnover | MTM drawdown |\n'
             '| --- | --- | ---: | ---: | ---: | ---: |']
    for result in results:
        rate = f'{result["pnl_per_million"]:,.2f}' if result['pnl_per_million'] is not None else 'N/A'
        lines.append(f'| {result["segment"]} | {result["name"]} | {result["gross_pnl"]:,.2f} | '
                     f'{result["turnover"]:,.2f} | {rate} | {result["max_drawdown_liquidation_mtm"]:,.2f} |')
    (output/'REPORT.md').write_text('\n'.join(lines)+'\n', encoding='utf-8')
    # Written last: its presence marks a successfully completed run.
    bundle = dict(input_manifest=manifest, input_raw_sha256=manifest['raw_sha256'],
                  start_utc=iso(start), end_utc_exclusive=iso(end), split_at=[iso(t) for t in split_at],
                  code_sha256=hashes, results=results)
    (output/'results.json').write_text(json.dumps(bundle, indent=2), encoding='utf-8')
    return bundle
