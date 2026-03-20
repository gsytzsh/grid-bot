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
import json
import os
from typing import List, Dict, Optional
from dataclasses import dataclass, field, asdict
from decimal import Decimal
from datetime import datetime
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

# 持久化文件路径
DATA_DIR = Path(__file__).parent.parent.parent / 'data'
GRIDS_FILE = DATA_DIR / 'grids.json'


class GridStatus(Enum):
    """网格状态"""
    ACTIVE = "active"
    STOPPED = "stopped"

    @classmethod
    def from_string(cls, value: str) -> 'GridStatus':
        return cls(value)


class LevelStatus(Enum):
    """网格级别状态"""
    PENDING = "pending"          # 等待挂单
    ORDER_PLACED = "order_placed"  # 已挂单，等待成交
    FILLED = "filled"            # 已成交
    CANCELLED = "cancelled"      # 已取消

    @classmethod
    def from_string(cls, value: str) -> 'LevelStatus':
        return cls(value)


def decimal_to_float(obj):
    """将 Decimal 转换为 float"""
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def float_to_decimal(obj):
    """将 float 转换为 Decimal"""
    if isinstance(obj, (int, float)):
        return Decimal(str(obj))
    return obj


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

    def to_dict(self) -> Dict:
        return {
            'level_id': self.level_id,
            'price': decimal_to_float(self.price),
            'order_type': self.order_type,
            'size': decimal_to_float(self.size),
            'status': self.status.value,
            'order_id': self.order_id,
            'filled_price': decimal_to_float(self.filled_price) if self.filled_price else None,
            'filled_time': self.filled_time.isoformat() if self.filled_time else None,
            'profit': decimal_to_float(self.profit)
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'GridLevel':
        return cls(
            level_id=data['level_id'],
            price=float_to_decimal(data['price']),
            order_type=data['order_type'],
            size=float_to_decimal(data['size']),
            status=LevelStatus.from_string(data['status']),
            order_id=data.get('order_id'),
            filled_price=float_to_decimal(data['filled_price']) if data.get('filled_price') else None,
            filled_time=datetime.fromisoformat(data['filled_time']) if data.get('filled_time') else None,
            profit=float_to_decimal(data.get('profit', 0))
        )


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

    def to_dict(self) -> Dict:
        return {
            'inst_id': self.inst_id,
            'lower_price': decimal_to_float(self.lower_price),
            'upper_price': decimal_to_float(self.upper_price),
            'grid_num': self.grid_num,
            'investment_amount': decimal_to_float(self.investment_amount),
            'stop_loss_price': decimal_to_float(self.stop_loss_price) if self.stop_loss_price else None,
            'take_profit_price': decimal_to_float(self.take_profit_price) if self.take_profit_price else None
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'GridConfig':
        return cls(
            inst_id=data['inst_id'],
            lower_price=float_to_decimal(data['lower_price']),
            upper_price=float_to_decimal(data['upper_price']),
            grid_num=data['grid_num'],
            investment_amount=float_to_decimal(data['investment_amount']),
            stop_loss_price=float_to_decimal(data['stop_loss_price']) if data.get('stop_loss_price') else None,
            take_profit_price=float_to_decimal(data['take_profit_price']) if data.get('take_profit_price') else None
        )


@dataclass
class Position:
    """持仓信息"""
    level_id: int
    coin_size: Decimal
    buy_price: Decimal
    target_sell_price: Decimal

    def to_dict(self) -> Dict:
        return {
            'level_id': self.level_id,
            'coin_size': decimal_to_float(self.coin_size),
            'buy_price': decimal_to_float(self.buy_price),
            'target_sell_price': decimal_to_float(self.target_sell_price)
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Position':
        return cls(
            level_id=data['level_id'],
            coin_size=float_to_decimal(data['coin_size']),
            buy_price=float_to_decimal(data['buy_price']),
            target_sell_price=float_to_decimal(data['target_sell_price'])
        )


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

    def to_dict(self) -> Dict:
        """转换为字典（用于持久化）"""
        return {
            'grid_id': self.grid_id,
            'config': self.config.to_dict(),
            'levels': [level.to_dict() for level in self.levels],
            'status': self.status.value,
            'created_time': self.created_time.isoformat(),
            'total_profit': decimal_to_float(self.total_profit),
            'total_trades': self.total_trades,
            'invested_amount': decimal_to_float(self.invested_amount),
            'current_value': decimal_to_float(self.current_value),
            'positions': {str(k): v.to_dict() for k, v in self.positions.items()}
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'GridInstance':
        """从字典加载"""
        grid = cls(
            grid_id=data['grid_id'],
            config=GridConfig.from_dict(data['config']),
            status=GridStatus.from_string(data['status']),
            created_time=datetime.fromisoformat(data['created_time']),
            total_profit=float_to_decimal(data.get('total_profit', 0)),
            total_trades=data.get('total_trades', 0),
            invested_amount=float_to_decimal(data.get('invested_amount', 0)),
            current_value=float_to_decimal(data.get('current_value', 0)),
            positions={int(k): Position.from_dict(v) for k, v in data.get('positions', {}).items()}
        )
        grid.levels = [GridLevel.from_dict(level) for level in data.get('levels', [])]
        return grid


class GridStrategy:
    """网格策略引擎"""

    def __init__(self):
        self.grids: Dict[str, GridInstance] = {}
        self.grid_counter = 0
        self._ensure_data_dir()
        self.load_grids()  # 启动时加载持久化的网格

    def _ensure_data_dir(self):
        """确保数据目录存在"""
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def save_grids(self):
        """保存网格到文件"""
        try:
            data = {
                'grid_counter': self.grid_counter,
                'grids': [grid.to_dict() for grid in self.grids.values()]
            }
            with open(GRIDS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.debug(f"已保存 {len(self.grids)} 个网格到 {GRIDS_FILE}")
        except Exception as e:
            logger.error(f"保存网格失败：{e}")

    def load_grids(self):
        """从文件加载网格"""
        if not GRIDS_FILE.exists():
            logger.info("未找到持久化的网格文件")
            return

        try:
            with open(GRIDS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.grid_counter = data.get('grid_counter', 0)
            for grid_data in data.get('grids', []):
                grid = GridInstance.from_dict(grid_data)
                self.grids[grid.grid_id] = grid
                # 恢复运行中的网格状态
                if grid.status == GridStatus.ACTIVE:
                    # 重置所有订单状态（因为重启后订单 ID 已失效）
                    for level in grid.levels:
                        level.order_id = None
                        if level.status == LevelStatus.ORDER_PLACED:
                            level.status = LevelStatus.PENDING
                        # FILLED 状态保持不变，保留 filled_price 信息
                        # 重启后会根据 filled_price 和持仓信息重新挂卖单
                    logger.info(f"恢复网格：{grid.grid_id} ({grid.config.inst_id}), 持仓数={len(grid.positions)}")

            logger.info(f"已加载 {len(self.grids)} 个网格")
        except Exception as e:
            logger.error(f"加载网格失败：{e}")

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
        self.save_grids()  # 持久化

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
            self.save_grids()  # 持久化
            return True
        return False

    def delete_grid(self, grid_id: str) -> bool:
        """删除网格"""
        if grid_id in self.grids:
            del self.grids[grid_id]
            self.save_grids()  # 持久化
            return True
        return False

    def get_all_grids(self) -> List[GridInstance]:
        """获取所有网格"""
        return list(self.grids.values())

    def update_grid(self, grid_id: str):
        """更新网格（触发持久化）"""
        if grid_id in self.grids:
            self.save_grids()

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
