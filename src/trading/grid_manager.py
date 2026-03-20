"""
网格交易管理器 - 修复版

正确的网格交易逻辑：
1. 启动时：在当前价下方的网格挂买单
2. 买单成交后：记录持仓，在上方对应价格挂卖单
3. 卖单成交后：计算利润，清除持仓，重新挂买单
4. 循环往复，持续赚取差价

状态流转：
  pending → order_placed → filled → pending
   (等待)    (已挂单)    (已成交)  (重置)
"""
import logging
from typing import Dict, List, Optional
from decimal import Decimal
from datetime import datetime
import asyncio

from ..api.okx_client import OKXClient
from ..strategy.grid_strategy import (
    GridStrategy, GridConfig, GridInstance, GridLevel, GridStatus, LevelStatus
)

logger = logging.getLogger(__name__)


class GridTradeManager:
    """网格交易管理器"""

    def __init__(self, client: OKXClient):
        self.client = client
        self.strategy = GridStrategy()
        self.running = False

    def create_grid(
        self,
        inst_id: str,
        lower_price: Decimal,
        upper_price: Decimal,
        grid_num: int,
        investment_amount: Decimal,
        stop_loss_price: Optional[Decimal] = None,
        take_profit_price: Optional[Decimal] = None
    ) -> Dict:
        """创建网格"""
        config = GridConfig(
            inst_id=inst_id,
            lower_price=lower_price,
            upper_price=upper_price,
            grid_num=grid_num,
            investment_amount=investment_amount,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price
        )

        # 验证配置
        if lower_price >= upper_price:
            return {"success": False, "message": "价格下限必须小于上限"}

        if grid_num < 2 or grid_num > 100:
            return {"success": False, "message": "网格数量必须在 2-100 之间"}

        # 检查最小投资金额（每格至少 5 USDT）
        # OKX 要求最小订单金额≥5 USDT
        min_investment = Decimal('5.5') * grid_num  # 每格 5.5 USDT，留 10% 余量
        if investment_amount < min_investment:
            return {"success": False, "message": f"最小投资金额需 {float(min_investment):.0f} USDT (每格≥5 USDT)"}

        # 检查当前价格是否在区间内
        ticker = self.client.get_ticker(inst_id)
        if ticker:
            current_price = Decimal(ticker.get('last', '0'))
            if current_price <= lower_price:
                return {"success": False, "message": f"当前价格 ({current_price}) 低于网格下限，可能立即满仓"}
            if current_price >= upper_price:
                return {"success": False, "message": f"当前价格 ({current_price}) 高于网格上限，可能立即空仓"}

        # 检查账户余额
        balance = self._check_usdt_balance()
        if balance < investment_amount * Decimal('1.1'):
            return {"success": False, "message": f"USDT 余额不足：可用 {balance}, 需要 {investment_amount * Decimal('1.1')}"}

        try:
            grid = self.strategy.create_grid(config)
            return {
                "success": True,
                "grid_id": grid.grid_id,
                "message": "网格创建成功"
            }
        except Exception as e:
            logger.error(f"创建网格失败：{e}")
            return {"success": False, "message": str(e)}

    def _check_usdt_balance(self) -> Decimal:
        """检查 USDT 可用余额"""
        try:
            balances = self.client.get_account_balance()
            for detail in balances:
                if detail.get('ccy') == 'USDT':
                    return Decimal(detail.get('availEq', '0'))
            return Decimal('0')
        except Exception:
            return Decimal('0')

    async def start_grid(self, grid_id: str) -> Dict:
        """启动网格"""
        grid = self.strategy.get_grid(grid_id)
        if not grid:
            return {"success": False, "message": "网格不存在"}

        if grid.status == GridStatus.ACTIVE:
            return {"success": False, "message": "网格已在运行中"}

        grid.status = GridStatus.ACTIVE

        # 获取当前价格
        ticker = self.client.get_ticker(grid.config.inst_id)
        if not ticker:
            return {"success": False, "message": "无法获取行情"}

        current_price = Decimal(ticker.get('last', '0'))
        if current_price == 0:
            current_price = Decimal(ticker.get('bidPx', '0'))

        logger.info(f"启动网格 {grid_id}, 当前价格：{current_price}")

        # 放置初始订单
        await self._place_initial_orders(grid, current_price)

        return {"success": True, "message": "网格已启动"}

    async def _place_initial_orders(self, grid: GridInstance, current_price: Decimal):
        """
        放置初始订单

        逻辑：在当前价下方的网格挂买单
        """
        for level in grid.levels:
            # 只处理等待中的级别
            if level.status != LevelStatus.PENDING:
                continue

            # 在当前价下方的网格挂买单（价格低于当前价）
            if level.price < current_price:
                result = await self._place_limit_order(
                    grid.config.inst_id,
                    "buy",
                    str(level.size),
                    str(level.price)
                )
                if result["success"]:
                    level.order_id = result["order_id"]
                    level.status = LevelStatus.ORDER_PLACED
                    logger.info(f"挂出买单：{grid.config.inst_id} @ {level.price}")
                else:
                    # 下单失败时标记为已挂单，防止重复下单
                    # 需要人工检查 OKX API 是否实际已挂出
                    logger.warning(f"下单失败：{grid.config.inst_id} @ {level.price} - {result.get('message')}")

    async def _place_limit_order(
        self,
        inst_id: str,
        side: str,
        size: str,
        price: str
    ) -> Dict:
        """挂限价单"""
        result = self.client.place_order(inst_id, side, size, price, order_type="limit")

        if result.success:
            return {"success": True, "order_id": result.order_id}
        else:
            return {"success": False, "message": result.message}

    async def _place_market_order(
        self,
        inst_id: str,
        side: str,
        size: str
    ) -> Dict:
        """市价单"""
        result = self.client.place_order(inst_id, side, size, order_type="market")

        if result.success:
            return {"success": True, "order_id": result.order_id}
        else:
            return {"success": False, "message": result.message}

    async def cancel_order(self, inst_id: str, order_id: str) -> bool:
        """撤销订单"""
        return self.client.cancel_order(inst_id, order_id)

    async def stop_grid(self, grid_id: str) -> Dict:
        """停止网格"""
        grid = self.strategy.get_grid(grid_id)
        if not grid:
            return {"success": False, "message": "网格不存在"}

        # 1. 先更新网格状态（确保前端能立即看到）
        self.strategy.stop_grid(grid_id)
        logger.info(f"停止网格 {grid_id}，状态已设置为 STOPPED")

        # 2. 更新本地订单状态（立即清除）
        for level in grid.levels:
            if level.order_id:
                level.status = LevelStatus.CANCELLED
                level.order_id = None

        # 3. 从 OKX API 获取并撤销所有挂单（异步执行，不影响状态）
        try:
            await self._cancel_all_orders_for_inst(grid.config.inst_id)
        except Exception as e:
            logger.error(f"撤销挂单异常：{e}")

        # 4. 卖出所有持仓（彻底清仓）
        if grid.positions:
            logger.info(f"网格 {grid_id} 有 {len(grid.positions)} 个持仓，开始平仓")
            try:
                await self._close_all_positions(grid)
            except Exception as e:
                logger.error(f"平仓异常：{e}")

        logger.info(f"网格已停止：{grid_id}")
        return {"success": True, "message": "网格已停止"}

    async def _cancel_all_orders_for_inst(self, inst_id: str):
        """撤销指定交易对的所有挂单"""
        try:
            result = self.client.trade_api.get_order_list(instType="SPOT")
            if result and result.get("data"):
                orders = result.get("data", [])
                for order in orders:
                    if order.get("instId") == inst_id and order.get("state") == "live":
                        order_id = order.get("ordId")
                        cancel_result = self.client.cancel_order(inst_id, order_id)
                        if cancel_result:
                            logger.info(f"撤销挂单：{inst_id} @ {order.get('px')} {order.get('side')} (orderId={order_id})")
                        else:
                            logger.warning(f"撤销挂单失败：{inst_id} orderId={order_id}")
        except Exception as e:
            logger.error(f"获取挂单列表失败：{e}")

    def delete_grid(self, grid_id: str) -> Dict:
        """删除网格"""
        grid = self.strategy.get_grid(grid_id)
        if not grid:
            return {"success": False, "message": "网格不存在"}

        # 检查网格是否还在运行
        if grid.status == GridStatus.ACTIVE:
            return {"success": False, "message": "网格正在运行，请先停止再删除"}

        # 检查是否有未撤销的挂单
        pending_orders = [l for l in grid.levels if l.status == LevelStatus.ORDER_PLACED]
        if pending_orders:
            return {"success": False, "message": f"网格还有 {len(pending_orders)} 个挂单未撤销，请先停止网格"}

        # 检查是否有持仓
        if grid.positions:
            return {"success": False, "message": f"网格还有 {len(grid.positions)} 个持仓未平仓，请先停止网格"}

        if self.strategy.delete_grid(grid_id):
            return {"success": True, "message": "网格已删除"}
        return {"success": False, "message": "网格不存在"}

    def get_grid_info(self, grid_id: str) -> Optional[Dict]:
        """获取网格信息"""
        grid = self.strategy.get_grid(grid_id)
        if not grid:
            return None

        # 更新当前价值
        ticker = self.client.get_ticker(grid.config.inst_id)
        if ticker:
            current_price = Decimal(ticker.get('last', '0'))
            grid.update_value(current_price)

        return {
            "grid_id": grid.grid_id,
            "inst_id": grid.config.inst_id,
            "status": grid.status.value,
            "lower_price": str(grid.config.lower_price),
            "upper_price": str(grid.config.upper_price),
            "grid_num": grid.config.grid_num,
            "investment": str(grid.invested_amount),
            "current_value": str(grid.current_value),
            "total_profit": str(grid.total_profit),
            "roi": str(grid.get_roi()),
            "total_trades": grid.total_trades,
            "created_time": grid.created_time.isoformat(),
            "positions": [
                {
                    "level_id": pos.level_id,
                    "size": str(pos.coin_size),
                    "buy_price": str(pos.buy_price),
                    "target_sell": str(pos.target_sell_price)
                }
                for pos in grid.positions.values()
            ],
            "levels": [
                {
                    "level": l.level_id + 1,
                    "price": str(l.price),
                    "type": l.order_type,
                    "status": l.status.value,
                    "size": str(l.size)
                }
                for l in grid.levels
            ]
        }

    def get_all_grids(self) -> List[Dict]:
        """获取所有网格"""
        grids = []
        for g in self.strategy.get_all_grids():
            # 更新当前价值
            ticker = self.client.get_ticker(g.config.inst_id)
            if ticker:
                current_price = Decimal(ticker.get('last', '0'))
                g.update_value(current_price)

            grids.append({
                "grid_id": g.grid_id,
                "inst_id": g.config.inst_id,
                "status": g.status.value,
                "total_profit": str(g.total_profit),
                "roi": str(g.get_roi()),
                "total_trades": g.total_trades,
                "current_value": str(g.current_value),
                "investment": str(g.invested_amount),
                "position_count": len(g.positions)
            })
        return grids

    def calculate_preview(
        self,
        lower_price: Decimal,
        upper_price: Decimal,
        grid_num: int
    ) -> List[Dict]:
        """预览网格价格"""
        return self.strategy.calculate_grid_levels(lower_price, upper_price, grid_num)

    async def monitor_and_trade(self):
        """监控网格并执行交易"""
        while self.running:
            try:
                for grid in self.strategy.get_all_grids():
                    if grid.status != GridStatus.ACTIVE:
                        continue

                    # 获取当前价格
                    ticker = self.client.get_ticker(grid.config.inst_id)
                    if not ticker:
                        continue

                    current_price = Decimal(ticker.get('last', '0'))
                    if current_price == 0:
                        continue

                    # 检查止损止盈
                    action = self.strategy.check_stop_loss_take_profit(grid.grid_id, current_price)
                    if action:
                        logger.info(f"网格 {grid.grid_id} 触发 {action}")
                        # 触发止损/止盈时直接停止（stop_grid 会处理平仓）
                        await self.stop_grid(grid.grid_id)
                        continue

                    # 检查订单状态和成交情况
                    await self._check_orders_and_trade(grid, current_price)

                await asyncio.sleep(2)  # 每 2 秒检查一次

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"网格监控错误：{e}")
                await asyncio.sleep(5)

    async def _check_orders_and_trade(self, grid: GridInstance, current_price: Decimal):
        """检查订单状态并执行交易"""

        for level in grid.levels:
            if level.status == LevelStatus.CANCELLED:
                continue

            # 【修复】FILLED 状态表示买单已成交，等待卖单成交，不重新挂买单
            if level.status == LevelStatus.FILLED:
                continue

            # 检查已挂出的订单是否成交
            if level.status == LevelStatus.ORDER_PLACED:
                if level.order_type == "sell":
                    await self._check_sell_order_filled(grid, level, current_price)
                else:
                    await self._check_buy_order_filled(grid, level, current_price)

            # 检查是否需要重新挂单
            elif level.status == LevelStatus.PENDING:
                await self._check_and_place_order(grid, level, current_price)

    async def _check_buy_order_filled(self, grid: GridInstance, level: GridLevel, current_price: Decimal):
        """检查买单是否成交"""
        # 买单逻辑：价格下跌时成交
        # 如果当前价格远高于挂单价，订单不太可能成交，但也不应该撤销
        # 网格交易的买单就是要在低价挂着等成交

        # 检查订单状态
        if level.order_id:
            order = self.client.get_order_status(grid.config.inst_id, level.order_id)
            if order:
                state = order.get('state', '')
                if state == '3':  # OKX: 3=完全成交
                    # 买单成交
                    filled_price = Decimal(order.get('avgPx', level.price))
                    await self._on_buy_filled(grid, level, filled_price)
                elif state == '4':  # OKX: 4=已撤单
                    level.status = LevelStatus.PENDING
                    level.order_id = None
                elif state == '5':  # OKX: 5=部分成交
                    # 部分成交也视为成交，继续处理
                    filled_price = Decimal(order.get('avgPx', level.price))
                    filled_size = Decimal(order.get('accFillSz', '0'))
                    if filled_size > 0:
                        level.size = filled_size  # 更新为实际成交量
                        await self._on_buy_filled(grid, level, filled_price)

    async def _on_buy_filled(self, grid: GridInstance, level: GridLevel, filled_price: Decimal):
        """买单成交后的处理"""
        # 获取目标卖出价格（上一格）
        sell_level_id = level.level_id + 1
        if sell_level_id < len(grid.levels):
            target_sell_price = grid.levels[sell_level_id].price
            sell_level = grid.levels[sell_level_id]

            # 记录持仓
            grid.add_position(
                level_id=level.level_id,  # 买单级别 ID 作为 key
                coin_size=level.size,
                buy_price=filled_price,
                target_sell_price=target_sell_price
            )

            logger.info(f"买单成交：{grid.config.inst_id} @ {filled_price}, 目标卖出：{target_sell_price}")

            # 【关键修复】买单成交后，不立即重置状态，标记为 FILLED
            # 等待卖单成交后，再重置买单状态，防止重复挂买单
            level.status = LevelStatus.FILLED
            level.order_id = None
            level.filled_price = filled_price

            # 检查目标卖单级别是否已有订单
            if sell_level.status == LevelStatus.ORDER_PLACED:
                # 已有订单，需要先撤销（可能是之前的买单）
                if sell_level.order_id:
                    await self.cancel_order(grid.config.inst_id, sell_level.order_id)
                    logger.info(f"撤销 {sell_level.order_type} 单：{grid.config.inst_id} @ {sell_level.price}")

            # 挂出卖单
            result = await self._place_limit_order(
                grid.config.inst_id,
                "sell",
                str(level.size),
                str(target_sell_price)
            )
            if result["success"]:
                # 更新卖单级别的状态
                sell_level.order_id = result["order_id"]
                sell_level.status = LevelStatus.ORDER_PLACED
                sell_level.order_type = "sell"
                logger.info(f"挂出卖单：{grid.config.inst_id} @ {target_sell_price}")

            # 持久化网格状态
            self.strategy.update_grid(grid.grid_id)
        else:
            # 已经是最上面一格，无法挂卖单，重置买单级别
            level.status = LevelStatus.PENDING
            level.order_id = None
            logger.info(f"买单成交于最高格：{grid.config.inst_id} @ {filled_price}, 等待价格上涨卖出")

    async def _on_sell_filled(self, grid: GridInstance, level: GridLevel, filled_price: Decimal):
        """卖单成交后的处理"""
        # 找到对应的持仓（当前级别 -1 是买单级别）
        buy_level_id = level.level_id - 1
        position = grid.get_position(buy_level_id)

        if position:
            # 计算利润
            profit = (filled_price - position.buy_price) * position.coin_size
            grid.total_profit += profit
            logger.info(f"卖单成交：{grid.config.inst_id} @ {filled_price}, 利润={profit}")

            # 清除持仓
            grid.remove_position(buy_level_id)

        # 重置卖单级别状态
        level.status = LevelStatus.PENDING
        level.order_id = None
        level.filled_price = None
        level.order_type = "buy"  # 恢复为买单类型

        grid.total_trades += 1

        # 【关键修复】卖单成交后，重置买单级别状态，允许重新挂买单
        buy_level = grid.levels[buy_level_id]
        buy_level.status = LevelStatus.PENDING
        buy_level.order_id = None
        buy_level.order_type = "buy"
        buy_level.filled_price = None  # 清除成交记录

        logger.info(f"卖单成交，准备重新挂买单：{grid.config.inst_id} @ {buy_level.price}")

        # 持久化网格状态
        self.strategy.update_grid(grid.grid_id)

    async def _check_sell_order_filled(self, grid: GridInstance, level: GridLevel, current_price: Decimal):
        """检查卖单是否成交"""
        # 卖单逻辑：价格上涨时成交
        # 如果当前价格远低于挂单价，订单不太可能成交，但也不应该撤销
        # 网格交易的卖单就是要在高价挂着等成交

        # 检查订单状态
        if level.order_id:
            order = self.client.get_order_status(grid.config.inst_id, level.order_id)
            if order:
                state = order.get('state', '')
                if state == '3':  # OKX: 3=完全成交
                    # 卖单成交
                    filled_price = Decimal(order.get('avgPx', level.price))
                    await self._on_sell_filled(grid, level, filled_price)
                elif state == '4':  # OKX: 4=已撤单
                    level.status = LevelStatus.PENDING
                    level.order_id = None
                elif state == '5':  # OKX: 5=部分成交
                    filled_price = Decimal(order.get('avgPx', level.price))
                    if filled_price > 0:
                        await self._on_sell_filled(grid, level, filled_price)

    async def _close_all_positions(self, grid: GridInstance):
        """平仓所有持仓（用于止损/止盈时）"""
        logger.info(f"开始平仓网格 {grid.grid_id} 的所有持仓")

        # 先撤销所有挂单
        for level in grid.levels:
            if level.order_id and level.status == LevelStatus.ORDER_PLACED:
                await self.cancel_order(grid.config.inst_id, level.order_id)
                level.order_id = None
                level.status = LevelStatus.PENDING
                logger.info(f"撤销挂单：{level.order_type} @ {level.price}")

        # 市价卖出所有持仓
        for position in list(grid.positions.values()):
            try:
                logger.info(f"平仓卖出：{position.coin_size} (买入价：{position.buy_price})")
                # 使用市价单快速卖出
                result = await self._place_market_order(
                    grid.config.inst_id,
                    "sell",
                    str(position.coin_size)
                )
                if result["success"]:
                    grid.remove_position(position.level_id)
            except Exception as e:
                logger.error(f"平仓失败：{e}")

        logger.info(f"平仓完成，最终利润：{grid.total_profit}")

    async def _check_and_place_order(self, grid: GridInstance, level: GridLevel, current_price: Decimal):
        """检查并挂单"""
        # 检查这个级别是否有买单持仓（需要挂卖单）
        position = grid.get_position(level.level_id)

        if position:
            # 有持仓，应该在目标价格挂卖单
            # 但卖单已经在 _on_buy_filled 中挂出了，这里只需要检查状态
            # 找到卖单所在的级别
            sell_level_id = level.level_id + 1
            if sell_level_id < len(grid.levels):
                sell_level = grid.levels[sell_level_id]
                # 检查卖单是否还在
                if sell_level.status != LevelStatus.ORDER_PLACED or sell_level.order_type != "sell":
                    # 卖单没了，先撤销这个级别可能存在的其他订单
                    if sell_level.status == LevelStatus.ORDER_PLACED and sell_level.order_id:
                        await self.cancel_order(grid.config.inst_id, sell_level.order_id)

                    # 重新挂卖单
                    result = await self._place_limit_order(
                        grid.config.inst_id,
                        "sell",
                        str(position.coin_size),
                        str(position.target_sell_price)
                    )
                    if result["success"]:
                        sell_level.order_id = result["order_id"]
                        sell_level.status = LevelStatus.ORDER_PLACED
                        sell_level.order_type = "sell"
                        logger.info(f"重新挂出卖单：{grid.config.inst_id} @ {position.target_sell_price}")
                    else:
                        logger.warning(f"挂卖单失败：{grid.config.inst_id} @ {position.target_sell_price} - {result.get('message')}")
        else:
            # 没有持仓，挂买单
            # 先检查这个级别是否已经有卖单（有卖单说明有对应持仓，不应该挂买单）
            if level.status == LevelStatus.ORDER_PLACED and level.order_type == "sell":
                return  # 已有卖单，跳过

            # 买单逻辑：只要当前价高于买单价，就应该挂单（等价格跌下来成交）
            if current_price > level.price:
                # 已有挂单，跳过
                if level.status == LevelStatus.ORDER_PLACED:
                    return

                result = await self._place_limit_order(
                    grid.config.inst_id,
                    "buy",
                    str(level.size),
                    str(level.price)
                )
                if result["success"]:
                    level.order_id = result["order_id"]
                    level.status = LevelStatus.ORDER_PLACED
                    level.order_type = "buy"
                    logger.info(f"挂出买单：{grid.config.inst_id} @ {level.price}")
                else:
                    logger.warning(f"挂买单失败：{grid.config.inst_id} @ {level.price} - {result.get('message')}")
