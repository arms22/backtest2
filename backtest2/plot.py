"""Plot liquidation P&L from saved results, fills and market data; no bot runs."""
import argparse
from array import array
import bisect
import gzip
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .audit import liquidation_price
from .data import iter_market_rows, load_market_manifest, parse_time


def load_pnl_series(data, results, cases=(), models=()):
    """Reconstruct one-second liquidation MTM, summing independent segments."""
    data, results = Path(data), Path(results)
    bundle = json.loads((results/'results.json').read_text(encoding='utf-8'))
    manifest = load_market_manifest(data)
    if manifest['raw_sha256'] != bundle['input_raw_sha256']:
        raise ValueError('Market data differs from the backtest input')
    selected = [r for r in bundle['results'] if (not cases or r['case'] in cases)
                and (not models or r['config']['model'] in models)]
    if not selected:
        raise ValueError('No results match the selected cases/models')
    start, end = parse_time(bundle['start_utc']), parse_time(bundle['end_utc_exclusive'])
    boundaries = [start, *map(parse_time, bundle['split_at']), end]
    curves, states = {}, {}
    for result in selected:
        name, segment = result['name'], result['segment']
        if name not in curves:
            curves[name] = dict(name=name, case=result['case'], model=result['config']['model'],
                                times=array('d', [start]), values=array('d', [0.]), offset=0.)
        with gzip.open(results/f'{segment}_{name}_fills.jsonl.gz', 'rt', encoding='utf-8') as source:
            fills = [json.loads(line) for line in source]
        states[segment, name] = dict(result=result, fills=fills, cursor=0, cash=0., sats=0)

    for row in iter_market_rows(data, start, end, manifest['product']):
        index = bisect.bisect_right(boundaries, row['second'])
        segment = f'segment_{index:03}'
        for name, curve in curves.items():
            state = states[segment, name]
            fills = state['fills']
            while state['cursor'] < len(fills) and fills[state['cursor']]['t'] <= row['timestamp']:
                fill = fills[state['cursor']]
                sign = 1 if fill['side'] == 'BUY' else -1
                state['sats'] += sign * round(fill['size'] * 1e8)
                state['cash'] -= sign * fill['size'] * fill['price']
                state['cursor'] += 1
            quantity, equity = state['sats']/1e8, state['cash']
            if quantity:
                equity += quantity * liquidation_price(row['bids'] if quantity > 0 else row['asks'],
                                                       abs(quantity))
            curve['times'].append(row['timestamp'])
            curve['values'].append(curve['offset'] + equity)
            if row['second'] + 1 == boundaries[index]:
                expected = state['result']['gross_pnl']
                if (state['cursor'] != len(fills) or state['sats'] != 0
                        or not math.isclose(equity, expected, abs_tol=1e-5)):
                    raise ValueError(f'Segment P&L or terminal position differs: {segment}/{name}')
                curve['offset'] += expected
                curve['times'].append(boundaries[index])
                curve['values'].append(curve['offset'])
    return bundle, list(curves.values())


def plot_pnl(data, results, output=None, cases=(), models=(), utc_offset=0):
    """Save a P&L figure (SVG by default); matplotlib is an optional dependency."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.ticker import StrMethodFormatter

    bundle, curves = load_pnl_series(data, results, cases, models)
    output = Path(output) if output else Path(results)/'pnl.svg'
    zone = timezone(timedelta(hours=utc_offset))
    panels = list(dict.fromkeys(c['model'] for c in curves))
    case_names = list(dict.fromkeys(c['case'] for c in curves))
    colors = {name:plt.get_cmap('tab10')(i % 10) for i,name in enumerate(case_names)}
    with plt.rc_context({'svg.fonttype':'none', 'font.size':10}):
        fig, axes = plt.subplots(len(panels), 1, figsize=(14, 4.8*len(panels)),
                                 squeeze=False, sharex=True, layout='constrained')
        try:
            product = bundle['input_manifest']['product']
            fig.suptitle(f'{product} — cumulative P&L', fontsize=17, fontweight='bold')
            for model, ax in zip(panels, axes[:, 0]):
                for curve in (c for c in curves if c['model'] == model):
                    times = [datetime.fromtimestamp(t, zone) for t in curve['times']]
                    label = f"{curve['case']}  ({curve['values'][-1]:+,.2f} JPY)"
                    ax.plot(times, curve['values'], label=label, color=colors[curve['case']],
                            linewidth=1.4)
                for split in bundle['split_at']:
                    ax.axvline(datetime.fromtimestamp(parse_time(split), zone), color='#94a3b8',
                               linestyle=':', linewidth=1)
                ax.axhline(0, color='#64748b', linestyle='--', linewidth=.8)
                ax.set_title(model, loc='left', fontsize=12)
                ax.set_ylabel('Liquidation P&L (JPY)')
                ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
                locator = mdates.AutoDateLocator(tz=zone)
                ax.xaxis.set_major_locator(locator)
                ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator, tz=zone))
                ax.grid(True, alpha=.18)
                ax.spines[['top', 'right']].set_visible(False)
                ax.legend(loc='upper left', bbox_to_anchor=(1.01, 1), frameon=False, fontsize=8)
            note = '1-second liquidation MTM; independent segments summed; costs as recorded'
            axes[-1, 0].set_xlabel(f"Time ({zone.tzname(None)})\n{note}")
            output.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output, dpi=150)
        finally:
            plt.close(fig)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, required=True, help='Saved market-data directory')
    parser.add_argument('--results', type=Path, required=True, help='Completed backtest directory')
    parser.add_argument('--output', type=Path, help='Figure path; default: <results>/pnl.svg')
    parser.add_argument('--case', action='append', default=[], help='Case filter; repeatable')
    parser.add_argument('--model', action='append', default=[], help='Model filter; repeatable')
    parser.add_argument('--utc-offset', type=float, default=0, help='Display time offset; JST is 9')
    args = parser.parse_args()
    try:
        output = plot_pnl(args.data, args.results, args.output, args.case, args.model, args.utc_offset)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(f'Saved P&L plot to {output}')


if __name__ == '__main__':
    main()
