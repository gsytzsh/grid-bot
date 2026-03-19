"""
交易管理器 - 处理订单执行和止盈止损
"""
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
import asyncio

from ..api.okx_client import OKXClient, OrderResult, Position

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """交易记录"""
    inst_id: str
    side: str
    size: Decimal
    price: Decimal
    order_id: str
    timestamp: datetime
    pnl: Decimal = Decimal('0')
    status: str = "open"  # open/closed


@dataclass
class PositionInfo:
    """持仓信息（带止盈止损）"""
    inst_id: str
    size: Decimal
    entry_price: Decimal
    current_price: Decimal = Decimal('0')
    stop_loss_price: Decimal = Decimal('0')
    take_profit_price: Decimal = Decimal('0')
    pnl: Decimal = Decimal('0')
    pnl_percent: Decimal = Decimal('0')
    timestamp: datetime = field(default_factory=datetime.now)

    def update_pnl(self, current_price: Decimal):
        """更新盈亏"""
        self.current_price = current_price
        self.pnl = (current_price - self.entry_price) * self.size
        self.pnl_percent = (current_price - self.entry_price) / self.entry_price * Decimal('100')

    def check_stop_loss(self) -> bool:
        """检查是否触发止损"""
        if self.stop_loss_price > 0 and self.current_price <= self.stop_loss_price:
            return True
        return False

    def check_take_profit(self) -> bool:
        """检查是否触发止盈"""
        if self.take_profit_price > 0 and self.current_price >= self.take_profit_price:
            return True
        return False


class TradingManager:
    """交易管理器"""

    def __init__(
        self,
        client: OKXClient,
        stop_loss_percent: Decimal = Decimal('2'),
        take_profit_percent: Decimal = Decimal('3')
    ):
        """
        初始化交易管理器

        Args:
            client: OKX 客户端
            stop_loss_percent: 止损百分比（如 2 表示 2%）
            take_profit_percent: 止盈百分比（如 3 表示 3%）
        """
        self.client = client
        self.stop_loss_percent = stop_loss_percent / Decimal('100')
        self.take_profit_percent = take_profit_percent / Decimal('100')

        # 持仓追踪
        self.positions: Dict[str, PositionInfo] = {}
        # 交易记录
        self.trade_history: List[TradeRecord] = []

    async def execute_arb_order(
        self,
        inst_id: str,
        side: str,
        size: str,
        price: Optional[str] = None
    ) -> OrderResult:
        """
        执行套利订单

        Args:
            inst_id: 交易对
            side: buy/sell
            size: 数量
            price: 价格（可选）

        Returns:
            订单结果
        """
        result = self.client.place_order(inst_id, side, size, price)

        if result.success:
            # 记录交易
            record = TradeRecord(
                inst_id=inst_id,
                side=side,
                size=Decimal(size),
                price=Decimal(price) if price else Decimal('0'),
                order_id=result.order_id,
                timestamp=datetime.now()
            )
            self.trade_history.append(record)

            # 如果是买入，创建持仓追踪
            if side == "buy":
                await self._track_position(inst_id, result.order_id)

        return result

    async def _track_position(self, inst_id: str, order_id: str):
        """追踪持仓，设置止盈止损"""
        # 等待订单成交
        await asyncio.sleep(1)

        # 获取订单状态
        order = self.client.get_order_status(inst_id, order_id)
        if not order:
            return

        # 获取当前价格
        ticker = self.client.get_ticker(inst_id)
        if not ticker:
            return

        current_price = Decimal(ticker.get('lastPx', '0'))
        if current_price == 0:
            current_price = Decimal(ticker.get('bidPx', '0'))

        # 计算成交数量
        filled_size = Decimal(order.get('accFillSz', '0'))
        if filled_size == 0:
            return

        avg_price = Decimal(order.get('avgPx', '0'))

        # 计算止盈止损价格
        stop_loss_price = avg_price * (1 - self.stop_loss_percent)
        take_profit_price = avg_price * (1 + self.take_profit_percent)

        # 创建持仓信息
        position = PositionInfo(
            inst_id=inst_id,
            size=filled_size,
            entry_price=avg_price,
            current_price=current_price,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price
        )

        self.positions[inst_id] = position
        logger.info(
            f"创建持仓追踪：{inst_id}, 入场价={avg_price}, "
            f"止损={stop_loss_price}, 止盈={take_profit_price}"
        )

    async def check_positions(self) -> List[str]:
        """
        检查所有持仓的止盈止损状态

        Returns:
            需要平仓的持仓列表
        """
        to_close = []

        for inst_id, position in list(self.positions.items()):
            # 更新当前价格
            ticker = self.client.get_ticker(inst_id)
            if ticker:
                current_price = Decimal(ticker.get('lastPx', '0'))
                position.update_pnl(current_price)

                # 检查止损
                if position.check_stop_loss():
                    logger.warning(f"触发止损：{inst_id}, 亏损={position.pnl_percent:.2f}%")
                    to_close.append(inst_id)
                    continue

                # 检查止盈
                if position.check_take_profit():
                    logger.info(f"触发止盈：{inst_id}, 盈利={position.pnl_percent:.2f}%")
                    to_close.append(inst_id)

        return to_close

    async def close_position(self, inst_id: str) -> bool:
        """
        平仓

        Args:
            inst_id: 交易对

        Returns:
            是否成功
        """
        position = self.positions.get(inst_id)
        if not position:
            return False

        # 卖出持仓
        result = self.client.place_order(
            inst_id=inst_id,
            side="sell",
            size=str(position.size),
            price=str(position.current_price)
        )

        if result.success:
            # 更新交易记录
            for record in reversed(self.trade_history):
                if record.inst_id == inst_id and record.status == "open":
                    record.status = "closed"
                    record.pnl = position.pnl
                    break

            # 移除持仓
            del self.positions[inst_id]
            logger.info(f"已平仓：{inst_id}, PnL={position.pnl}")
            return True

        return False

    def get_all_positions(self) -> List[PositionInfo]:
        """获取所有持仓"""
        return list(self.positions.values())

    def get_total_pnl(self) -> Decimal:
        """获取总盈亏"""
        return sum(p.pnl for p in self.positions.values())

    def get_trade_history(self, limit: int = 50) -> List[TradeRecord]:
        """获取交易历史"""
        return self.trade_history[-limit:]
