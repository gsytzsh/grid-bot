"""
网格交易策略引擎

网格交易原理：
- 在设定的价格区间内分成若干网格
- 价格下跌时买入，价格上涨时卖出
- 每格赚取固定比例的差价

状态流转：
pending → order_placed → filled → pending (循环)
        (已挂单)    (已成交)  (重置)
"""
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class GridStatus(Enum):
    """网格状态"""
    ACTIVE = "active"
    STOPPED = "stopped"


class LevelStatus(Enum):
    """网格级别状态"""
    PENDING = "pending"          # 等待挂单
    ORDER_PLACED = "order_placed"  # 已挂单，等待成交
    FILLED = "filled"            # 已成交
    CANCELLED = "cancelled"      # 已取消


@dataclass
class GridLevel:
    """单个网格"""
    level_id: int
    price: Decimal  # 网格价格
    order_type: str  # "buy" 或 "sell"
    size: Decimal  # 订单大小（币种数量）
    status: LevelStatus = LevelStatus.PENDING
    order_id: Optional[str] = None
    filled_price: Optional[Decimal] = None
    filled_time: Optional[datetime] = None
    profit: Decimal = Decimal('0')  # 该格利润（仅 sell 格）


@dataclass
class GridConfig:
    """网格配置"""
    inst_id: str  # 交易对
    lower_price: Decimal  # 价格下限
    upper_price: Decimal  # 价格上限
    grid_num: int  # 网格数量
    investment_amount: Decimal  # 投资金额 (USDT)
    stop_loss_price: Optional[Decimal] = None  # 止损价
    take_profit_price: Optional[Decimal] = None  # 止盈价


@dataclass
class Position:
    """持仓信息"""
    level_id: int
    coin_size: Decimal
    buy_price: Decimal
    target_sell_price: Decimal


@dataclass
class GridInstance:
    """运行中的网格实例"""
    grid_id: str
    config: GridConfig
    levels: List[GridLevel] = field(default_factory=list)
    status: GridStatus = GridStatus.ACTIVE
    created_time: datetime = field(default_factory=datetime.now)
    total_profit: Decimal = Decimal('0')
    total_trades: int = 0
    invested_amount: Decimal = Decimal('0')
    current_value: Decimal = Decimal('0')
    # 持仓追踪：level_id -> Position
    positions: Dict[int, Position] = field(default_factory=dict)

    def update_value(self, current_price: Decimal):
        """更新当前价值"""
        # 计算持仓总价值
        total_coin = sum(pos.coin_size for pos in self.positions.values())
        spent_usdt = sum(pos.coin_size * pos.buy_price for pos in self.positions.values())

        # 当前价值 = 持仓币价值 + 剩余 USDT
        self.current_value = total_coin * current_price + (self.invested_amount - spent_usdt + self.total_profit)

    def get_roi(self) -> Decimal:
        """计算收益率"""
        if self.invested_amount == 0:
            return Decimal('0')
        return self.total_profit / self.invested_amount * Decimal('100')

    def add_position(self, level_id: int, coin_size: Decimal, buy_price: Decimal, target_sell_price: Decimal):
        """添加持仓"""
        self.positions[level_id] = Position(
            level_id=level_id,
            coin_size=coin_size,
            buy_price=buy_price,
            target_sell_price=target_sell_price
        )

    def remove_position(self, level_id: int) -> Optional[Position]:
        """移除持仓"""
        return self.positions.pop(level_id, None)

    def get_position(self, level_id: int) -> Optional[Position]:
        """获取持仓"""
        return self.positions.get(level_id)


class GridStrategy:
    """网格策略引擎"""

    def __init__(self):
        self.grids: Dict[str, GridInstance] = {}
        self.grid_counter = 0

    def create_grid(self, config: GridConfig) -> GridInstance:
        """
        创建网格

        网格分配逻辑：
        - 所有网格都是"买单"，价格从低到高
        - 启动时，在当前价下方的网格挂买单
        - 买单成交后，在更高一格挂卖单（卖出价 = 下一格价格）
        - 卖单成交后，重新在原买单价挂买单
        """
        self.grid_counter += 1
        grid_id = f"grid_{self.grid_counter}_{datetime.now().strftime('%H%M%S')}"

        # 计算网格价格
        price_range = config.upper_price - config.lower_price
        grid_step = price_range / config.grid_num

        # 计算每格订单大小
        # 按最低价格计算，确保每格都有足够的资金
        min_price = config.lower_price
        total_size = config.investment_amount / config.lower_price * Decimal('0.9')  # 留 10% 余量
        size_per_grid = total_size / config.grid_num

        # 验证最小订单大小（OKX 最小 5 USDT）
        min_order_usdt = size_per_grid * config.lower_price
        if min_order_usdt < Decimal('5'):
            raise ValueError(f"每格订单太小 ({min_order_usdt} USDT)，需要≥5 USDT。100 USDT 建议最多 18 格")

        levels = []
        for i in range(config.grid_num):
            price = config.lower_price + (grid_step * i)

            # 所有网格都标记为"buy"类型
            # 实际交易时，买单成交后会在更高价格挂"sell"单
            level = GridLevel(
                level_id=i,
                price=price.quantize(Decimal('0.01')),
                order_type="buy",  # 初始都是买单
                size=size_per_grid.quantize(Decimal('0.0001'))
            )
            levels.append(level)

        grid = GridInstance(
            grid_id=grid_id,
            config=config,
            levels=levels,
            invested_amount=config.investment_amount
        )

        self.grids[grid_id] = grid
        logger.info(f"创建网格：{grid_id}, 交易对={config.inst_id}, "
                   f"区间={config.lower_price}-{config.upper_price}, 格数={config.grid_num}")

        return grid

    def get_grid(self, grid_id: str) -> Optional[GridInstance]:
        """获取网格实例"""
        return self.grids.get(grid_id)

    def stop_grid(self, grid_id: str) -> bool:
        """停止网格"""
        grid = self.grids.get(grid_id)
        if grid:
            grid.status = GridStatus.STOPPED
            logger.info(f"停止网格：{grid_id}")
            return True
        return False

    def delete_grid(self, grid_id: str) -> bool:
        """删除网格"""
        if grid_id in self.grids:
            del self.grids[grid_id]
            return True
        return False

    def get_all_grids(self) -> List[GridInstance]:
        """获取所有网格"""
        return list(self.grids.values())

    def calculate_grid_levels(
        self,
        lower_price: Decimal,
        upper_price: Decimal,
        grid_num: int
    ) -> List[Dict]:
        """计算网格价格（用于预览）"""
        price_range = upper_price - lower_price
        grid_step = price_range / grid_num

        levels = []
        for i in range(grid_num):
            price = lower_price + (grid_step * i)
            # 预览时只显示价格，实际买卖类型取决于当前价格
            # 所有网格初始都是买单，买单成交后在上一格挂卖单
            levels.append({
                'level': i + 1,
                'price': float(price.quantize(Decimal('0.01'))),
                'type': 'buy'  # 初始都是买单
            })

        return levels

    def get_target_sell_price(self, grid: GridInstance, buy_level_id: int) -> Optional[Decimal]:
        """获取买入单对应的目标卖出价格"""
        # 上一格就是卖出目标
        sell_level_id = buy_level_id + 1
        if sell_level_id < len(grid.levels):
            return grid.levels[sell_level_id].price
        return None

    def check_stop_loss_take_profit(
        self,
        grid_id: str,
        current_price: Decimal
    ) -> Optional[str]:
        """检查止损止盈"""
        grid = self.grids.get(grid_id)
        if not grid or grid.status != GridStatus.ACTIVE:
            return None

        config = grid.config

        if config.stop_loss_price and current_price <= config.stop_loss_price:
            return "stop_loss"

        if config.take_profit_price and current_price >= config.take_profit_price:
            return "take_profit"

        return None
