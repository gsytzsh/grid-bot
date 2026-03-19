"""
套利策略引擎

实现三角套利和跨交易对套利策略
"""
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ArbOpportunity:
    """套利机会"""
    pair1: str  # 交易对 1 (如 BTC/USDT)
    pair2: str  # 交易对 2 (如 ETH/USDT)
    spread: Decimal  # 价差百分比
    direction: str  # buy/sell
    expected_profit: Decimal  # 预期利润
    timestamp: datetime


@dataclass
class TriangularArbOpportunity:
    """三角套利机会"""
    path: List[str]  # 套利路径，如 [USDT, BTC, ETH, USDT]
    pairs: List[str]  # 交易对列表
    expected_profit: Decimal  # 预期利润率
    timestamp: datetime


class ArbitrageEngine:
    """套利引擎"""

    def __init__(self, min_spread: Decimal = Decimal("0.003")):
        """
        初始化套利引擎

        Args:
            min_spread: 最小价差阈值，默认 0.3%
        """
        self.min_spread = min_spread
        self.opportunities: List[ArbOpportunity] = []
        self.triangular_opportunities: List[TriangularArbOpportunity] = []

    def update_opportunities(self, tickers: Dict[str, Dict]) -> List[ArbOpportunity]:
        """
        更新套利机会列表

        Args:
            tickers: 行情数据字典 {inst_id: ticker_data}

        Returns:
            发现的套利机会列表
        """
        self.opportunities = []

        # 寻找相关交易对（USDT 结尾）
        usdt_pairs = {k: v for k, v in tickers.items() if k.endswith('-USDT')}

        # 两两比较寻找套利机会
        pairs = list(usdt_pairs.keys())
        for i, pair1 in enumerate(pairs):
            for pair2 in pairs[i+1:]:
                opportunity = self._check_arb_opportunity(pair1, pair2, usdt_pairs)
                if opportunity:
                    self.opportunities.append(opportunity)

        return self.opportunities

    def _check_arb_opportunity(
        self,
        pair1: str,
        pair2: str,
        tickers: Dict[str, Dict]
    ) -> Optional[ArbOpportunity]:
        """检查两个交易对之间是否存在套利机会"""
        t1 = tickers.get(pair1)
        t2 = tickers.get(pair2)

        if not t1 or not t2:
            return None

        # 获取价格
        bid1 = Decimal(t1.get('bidPx', '0'))
        ask1 = Decimal(t1.get('askPx', '0'))
        bid2 = Decimal(t2.get('bidPx', '0'))
        ask2 = Decimal(t2.get('askPx', '0'))

        if bid1 == 0 or ask1 == 0 or bid2 == 0 or ask2 == 0:
            return None

        # 计算价差（简化版：比较相对强弱）
        # 实际套利需要更复杂的计算，这里做简化处理
        mid1 = (bid1 + ask1) / 2
        mid2 = (bid2 + ask2) / 2

        # 这里只是示例，实际三角套利需要计算完整路径
        # 例如：USDT -> BTC -> ETH -> USDT
        spread = abs(mid1 - mid2) / mid1 if mid1 > 0 else Decimal('0')

        if spread >= self.min_spread:
            direction = "buy" if mid1 < mid2 else "sell"
            return ArbOpportunity(
                pair1=pair1,
                pair2=pair2,
                spread=spread,
                direction=direction,
                expected_profit=spread * Decimal('0.98'),  # 扣除手续费
                timestamp=datetime.now()
            )

        return None

    def find_triangular_arb(
        self,
        tickers: Dict[str, Dict],
        base_currency: str = "USDT"
    ) -> List[TriangularArbOpportunity]:
        """
        寻找三角套利机会

        三角套利原理：
        USDT -> BTC -> ETH -> USDT
        如果最终 USDT 数量 > 初始 USDT 数量，则存在套利机会

        Args:
            tickers: 行情数据
            base_currency: 基础货币

        Returns:
            三角套利机会列表
        """
        opportunities = []

        # 获取所有相关交易对
        base_pairs = [k for k in tickers.keys() if k.endswith(f'-{base_currency}')]

        if len(base_pairs) < 2:
            return []

        # 检查所有可能的三角路径
        for i, pair1 in enumerate(base_pairs):
            for pair2 in base_pairs[i+1:]:
                # 提取币种
                base1 = pair1.replace(f'-{base_currency}', '')
                base2 = pair2.replace(f'-{base_currency}', '')

                # 检查是否存在 base1-base2 交易对
                cross_pair = f'{base1}-{base2}'
                cross_pair_rev = f'{base2}-{base1}'

                if cross_pair in tickers:
                    opp = self._calculate_triangular_profit(
                        path=[base_currency, base1, base2, base_currency],
                        pairs=[pair1, cross_pair, f'{base2}-{base_currency}'],
                        tickers=tickers
                    )
                    if opp and opp.expected_profit >= self.min_spread:
                        opportunities.append(opp)

                elif cross_pair_rev in tickers:
                    opp = self._calculate_triangular_profit(
                        path=[base_currency, base2, base1, base_currency],
                        pairs=[pair2, cross_pair_rev, f'{base1}-{base_currency}'],
                        tickers=tickers
                    )
                    if opp and opp.expected_profit >= self.min_spread:
                        opportunities.append(opp)

        self.triangular_opportunities = opportunities
        return opportunities

    def _calculate_triangular_profit(
        self,
        path: List[str],
        pairs: List[str],
        tickers: Dict[str, Dict]
    ) -> Optional[TriangularArbOpportunity]:
        """计算三角套利利润"""
        # 假设初始金额为 1000 USDT
        initial_amount = Decimal('1000')
        amount = initial_amount

        for pair in pairs:
            ticker = tickers.get(pair)
            if not ticker:
                return None

            ask = Decimal(ticker.get('askPx', '0'))
            bid = Decimal(ticker.get('bidPx', '0'))

            if ask == 0 or bid == 0:
                return None

            # 买入用 ask 价，卖出用 bid 价
            # 简化计算：用中间价
            mid = (ask + bid) / 2
            amount = amount / mid if mid > 0 else Decimal('0')

        # 最后一步换回 base_currency
        final_pair = pairs[-1]
        ticker = tickers.get(final_pair)
        if ticker:
            bid = Decimal(ticker.get('bidPx', '0'))
            if bid > 0:
                amount = amount * bid

        profit_rate = (amount - initial_amount) / initial_amount

        if profit_rate > 0:
            return TriangularArbOpportunity(
                path=path,
                pairs=pairs,
                expected_profit=profit_rate,
                timestamp=datetime.now()
            )

        return None

    def get_best_opportunity(self) -> Optional[ArbOpportunity]:
        """获取最佳套利机会"""
        if not self.opportunities:
            return None
        return max(self.opportunities, key=lambda x: x.expected_profit)

    def get_best_triangular_opportunity(self) -> Optional[TriangularArbOpportunity]:
        """获取最佳三角套利机会"""
        if not self.triangular_opportunities:
            return None
        return max(self.triangular_opportunities, key=lambda x: x.expected_profit)
