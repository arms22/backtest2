"""Independently audit saved replay fills and liquidation MTM, without running a bot."""
import argparse
import bisect
import gzip
import hashlib
import json
import math
from collections import defaultdict, deque
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


def _independent_fifo_matches(fills):
    """Rebuild close slices separately from the runner's analysis code."""
    lots = deque()
    matches = []
    open_before_terminal = None

    def open_records(as_of):
        return [dict(entry_fill_id=lot['fill_id'], size=lot['remaining'], as_of=as_of)
                for lot in lots if lot['remaining'] > 1e-10]

    for index, fill in enumerate(fills, start=1):
        fill_id = fill.get('fill_id', index)
        if fill['kind'] == 'terminal' and open_before_terminal is None:
            open_before_terminal = open_records('before_terminal')
        remaining = round(float(fill['size']), 8)
        while remaining > 1e-10 and lots and lots[0]['side'] != fill['side']:
            lot = lots[0]
            closed = round(min(remaining, lot['remaining']), 8)
            pnl = (fill['price'] - lot['price']) * closed
            if lot['side'] == 'SELL':
                pnl = -pnl
            matches.append(dict(entry_fill_id=lot['fill_id'], exit_fill_id=fill_id,
                                size=closed, gross_realized_pnl=pnl))
            lot['remaining'] = round(lot['remaining'] - closed, 8)
            remaining = round(remaining - closed, 8)
            if lot['remaining'] <= 1e-10:
                lots.popleft()
        if remaining > 1e-10:
            lots.append(dict(fill_id=fill_id, side=fill['side'], price=fill['price'],
                             remaining=remaining))
    if open_before_terminal is None:
        open_before_terminal = open_records('end_of_ledger')
    return matches, open_before_terminal


def _audit_match_files(output, key, fills, result, cash):
    matches_path = output/f'{key[0]}_{key[1]}_matches.jsonl.gz'
    open_path = output/f'{key[0]}_{key[1]}_open_lots.jsonl.gz'
    if not matches_path.exists() and not open_path.exists():
        return None
    assert matches_path.exists() and open_path.exists(), key
    with gzip.open(matches_path, 'rt', encoding='utf-8') as source:
        matches = [json.loads(line) for line in source]
    with gzip.open(open_path, 'rt', encoding='utf-8') as source:
        open_lots = [json.loads(line) for line in source]

    expected, expected_open = _independent_fifo_matches(fills)
    fills_by_id = {fill.get('fill_id', index): fill
                   for index, fill in enumerate(fills, start=1)}
    assert len(fills_by_id) == len(fills), key
    assert len(matches) == len(expected), key
    numeric_fields = ('entry_time', 'exit_time', 'entry_price', 'exit_price',
                      'size', 'holding_seconds', 'gross_realized_pnl', 'turnover')
    string_fields = ('entry_order_id', 'exit_order_id', 'entry_side', 'exit_side',
                     'position_direction', 'entry_kind', 'exit_kind')
    for actual, rebuilt in zip(matches, expected):
        assert actual['entry_fill_id'] == rebuilt['entry_fill_id'], key
        assert actual['exit_fill_id'] == rebuilt['exit_fill_id'], key
        entry = fills_by_id[actual['entry_fill_id']]
        exit_fill = fills_by_id[actual['exit_fill_id']]
        expected_values = dict(
            entry_order_id=entry['order'], exit_order_id=exit_fill['order'],
            entry_time=entry['t'], exit_time=exit_fill['t'],
            entry_price=entry['price'], exit_price=exit_fill['price'],
            entry_side=entry['side'], exit_side=exit_fill['side'],
            position_direction='long' if entry['side'] == 'BUY' else 'short',
            entry_kind=entry['kind'], exit_kind=exit_fill['kind'],
            holding_seconds=exit_fill['t'] - entry['t'],
            turnover=actual['size'] * (entry['price'] + exit_fill['price']),
        )
        for field in string_fields:
            assert actual[field] == expected_values[field], key
        for field in numeric_fields:
            expected_value = (rebuilt[field] if field in rebuilt else expected_values[field])
            assert math.isclose(actual[field], expected_value, abs_tol=1e-8), key
    assert len(open_lots) == len(expected_open), key
    for actual, rebuilt in zip(open_lots, expected_open):
        assert actual['entry_fill_id'] == rebuilt['entry_fill_id'], key
        assert actual['as_of'] == rebuilt['as_of'], key
        assert math.isclose(actual['size'], rebuilt['size'], abs_tol=1e-8), key
        entry = fills_by_id[actual['entry_fill_id']]
        assert actual['entry_order_id'] == entry['order'], key
        assert math.isclose(actual['entry_time'], entry['t'], abs_tol=1e-8), key
        assert math.isclose(actual['entry_price'], entry['price'], abs_tol=1e-8), key
        assert actual['entry_side'] == entry['side'], key
        assert actual['position_direction'] == ('long' if entry['side'] == 'BUY' else 'short'), key
        assert actual['entry_kind'] == entry['kind'], key

    realized_by_exit = defaultdict(float)
    allocated_by_fill = defaultdict(float)
    for match in matches:
        entry = fills_by_id[match['entry_fill_id']]
        exit_fill = fills_by_id[match['exit_fill_id']]
        assert entry['side'] != exit_fill['side'], key
        assert entry['t'] <= exit_fill['t'], key
        calculated = ((exit_fill['price'] - entry['price']) * match['size']
                      * (1 if entry['side'] == 'BUY' else -1))
        assert math.isclose(calculated, match['gross_realized_pnl'], abs_tol=1e-8), key
        realized_by_exit[match['exit_fill_id']] += calculated
        allocated_by_fill[match['exit_fill_id']] += match['size']
        allocated_by_fill[match['entry_fill_id']] += match['size']
    assert math.isclose(math.fsum(m['gross_realized_pnl'] for m in matches), cash,
                        abs_tol=1e-5), key
    assert math.isclose(math.fsum(m['turnover'] for m in matches), result['turnover'],
                        abs_tol=1e-4), key
    for index, fill in enumerate(fills, start=1):
        fill_id = fill.get('fill_id', index)
        assert math.isclose(realized_by_exit[fill_id], fill['realized_pnl'], abs_tol=1e-5), key
        assert math.isclose(allocated_by_fill[fill_id], fill['size'], abs_tol=1e-8), key
    assert result['match_slice_count'] == len(matches), key
    assert math.isclose(result['match_realized_pnl'], cash, abs_tol=1e-5), key
    return dict(match_slices=len(matches), open_lots_at_cutoff=len(open_lots))


def audit(data, output):
    bundle = json.loads((output/'results.json').read_text(encoding='utf-8'))
    raw_path = data/bundle.get('input_data_file', 'market_rows.jsonl.gz')
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
        match_check = _audit_match_files(output, key, fills, result, cash)
        states[key] = dict(result=result, fills=fills, match_check=match_check,
                           cursor=0, cash=0., sats=0,
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
                           fills=len(state['fills']), max_drawdown=state['dd'], flat=True,
                           **(state['match_check'] or {})))
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
