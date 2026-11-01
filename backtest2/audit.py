"""Independently audit saved replay fills and liquidation MTM, without running a bot."""
import argparse
import bisect
import gzip
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path

from .data import file_hash, parse_time


def liquidation_price(book, quantity):
    remaining, notional = quantity, 0.
    for price, available in book:
        take = min(remaining, available)
        notional += price * take
        remaining -= take
        if remaining <= 1e-10:
            return notional / quantity
    raise ValueError('Insufficient depth for independent liquidation')


def audit(data, output):
    bundle = json.loads((output/'results.json').read_text(encoding='utf-8'))
    raw_path = data/'market_rows.jsonl.gz'
    assert file_hash(raw_path) == bundle['input_raw_sha256'], 'Market data hash differs'
    start, end = parse_time(bundle['start_utc']), parse_time(bundle['end_utc_exclusive'])
    boundaries = [start, *map(parse_time, bundle['split_at']), end]
    states = {}
    for result in bundle['results']:
        key = (result['segment'], result['name'])
        assert key not in states
        assert result['fees'] == 0, 'This audit currently supports zero execution fees'
        with gzip.open(output/f'{key[0]}_{key[1]}_fills.jsonl.gz', 'rt', encoding='utf-8') as source:
            fills = [json.loads(line) for line in source]
        assert len(fills) == result['counts'].get('fill_events', 0)
        assert all(a['t'] <= b['t'] for a, b in zip(fills, fills[1:]))
        assert all(f['size'] > 0 and f['price'] > 0 and f['decision'] <= f['arrival'] <= f['t'] for f in fills)
        cash = math.fsum((1 if f['side']=='SELL' else -1)*f['size']*f['price'] for f in fills)
        turnover = math.fsum(f['size']*f['price'] for f in fills)
        sats = sum((1 if f['side']=='BUY' else -1)*round(f['size']*1e8) for f in fills)
        assert sats == 0, key
        assert math.isclose(cash, result['gross_pnl'], abs_tol=1e-5), key
        assert math.isclose(turnover, result['turnover'], abs_tol=1e-4), key
        assert math.isclose(cash, math.fsum(f['realized_pnl'] for f in fills), abs_tol=1e-5), key
        terminal = [f for f in fills if f['kind'] == 'terminal']
        assert len(terminal) <= 1
        assert bool(terminal) == (abs(result['terminal_position']) > 1e-9)
        if terminal:
            assert terminal[0] is fills[-1]
            assert math.isclose(terminal[0]['size'], abs(result['terminal_position']), abs_tol=1e-8)
        states[key] = dict(result=result, fills=fills, cursor=0, cash=0., sats=0,
                           peak=0., dd=0., previous_equity=0., daily={})
    with gzip.open(raw_path, 'rt', encoding='utf-8') as source:
        for line in source:
            row = json.loads(line)
            timestamp = datetime.fromisoformat(row['time'].replace('Z', '+00:00')).timestamp()
            second = math.floor(timestamp)
            if second < start:
                continue
            if second >= end:
                break
            segment = f'segment_{bisect.bisect_right(boundaries, second):03}'
            day = datetime.fromtimestamp(second+9*3600, timezone.utc).strftime('%Y-%m-%d')
            bids, asks = json.loads(row['bids_raw']), json.loads(row['asks_raw'])
            for (label, name), state in states.items():
                if label != segment:
                    continue
                fills = state['fills']
                while state['cursor'] < len(fills) and fills[state['cursor']]['t'] <= timestamp:
                    fill = fills[state['cursor']]
                    sign = 1 if fill['side']=='BUY' else -1
                    state['sats'] += sign * round(fill['size']*1e8)
                    state['cash'] -= sign * fill['size'] * fill['price']
                    state['cursor'] += 1
                quantity = state['sats']/1e8
                equity = state['cash']
                if quantity:
                    equity += quantity * liquidation_price(bids if quantity>0 else asks, abs(quantity))
                state['peak'] = max(state['peak'], equity)
                state['dd'] = max(state['dd'], state['peak']-equity)
                state['daily'][day] = state['daily'].get(day, 0.) + equity - state['previous_equity']
                state['previous_equity'] = equity
    checks = []
    for key, state in states.items():
        result = state['result']
        assert state['cursor'] == len(state['fills']) and state['sats'] == 0, key
        assert math.isclose(state['dd'], result['max_drawdown_liquidation_mtm'], abs_tol=1e-5), key
        assert set(state['daily']) == set(result['daily_pnl']), key
        assert all(math.isclose(pnl, state['daily'][day], abs_tol=1e-5)
                   for day, pnl in result['daily_pnl'].items()), key
        checks.append(dict(segment=key[0], name=key[1], pnl=state['cash'],
                           fills=len(state['fills']), max_drawdown=state['dd'], flat=True))
    record = dict(input_raw_sha256=bundle['input_raw_sha256'], results_sha256=file_hash(output/'results.json'),
                  audit_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), checks=checks)
    (output/'audit_checks.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
    print(f'Independent ledger/MTM/daily audit passed for {len(checks)} segment-case-model combinations')
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--results', type=Path, required=True)
    args = parser.parse_args()
    audit(args.data, args.results)


if __name__ == '__main__':
    main()
