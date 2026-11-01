"""FIFO accounting for zero-initial-position, zero-fee replays."""
from collections import deque
from math import fsum


class Position:

    def __init__(self):
        self.positions = deque()
        self.compute_summary()
        self.netprofit = 0
        self.commission_total = 0

    def add(self, p):
        """建玉追加"""
        self.commission_total += p['commission'] * p['price']
        self.positions.append(p.copy())
        while len(self.positions)>=2:
            r = self.positions.pop()
            l = self.positions.popleft()
            if r['side']==l['side']:
                # 売買方向が同じなら取り出したポジションを戻す
                self.positions.append(r)
                self.positions.appendleft(l)
                break
            else:
                if l['size'] >= r['size']:
                    pnl = (r['price']-l['price'])*r['size']
                    # 決済
                    l['size'] = round(l['size'] - r['size'],8)
                    if l['size'] > 0:
                        # サイズが残っている場合、ポジションを戻す
                        self.positions.appendleft(l)
                else:
                    pnl = (r['price']-l['price'])*l['size']
                    # 決済
                    r['size'] = round(r['size'] - l['size'],8)
                    if r['size'] > 0:
                        # サイズが残っている場合、ポジションを戻す
                        self.positions.append(r)
                if l['side']=='SELL':
                    pnl *= -1
                self.netprofit += pnl

        # 建玉情報更新
        self.compute_summary()

    def compute_summary(self):
        """サマリー計算"""
        positions = list(self.positions)
        if len(positions):
            ltp = positions[-1]['price']
            self.long_size = round(fsum(p['size'] for p in positions if p['side']=='BUY'),8)
            self.short_size = round(fsum(p['size'] for p in positions if p['side']=='SELL'),8)
            self.position_avg_price = fsum(p['price']*p['size'] for p in positions)/fsum(p['size'] for p in positions)
            self.position_size = self.long_size - self.short_size
            self.position_pnl = (ltp - self.position_avg_price) * self.position_size
        else:
            self.long_size = 0
            self.short_size = 0
            self.position_avg_price = 0
            self.position_size = 0
            self.position_pnl = 0
