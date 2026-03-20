"""
网格交易管理器

核心闭环：
1. 在当前价下方挂买单
2. 买单成交后创建持仓，并挂对应卖单
3. 卖单成交后计算利润，重置该买单层
4. 周期循环
"""
import logging
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
import asyncio

from ..api.okx_client import OKXClient
from ..strategy.grid_strategy import (
    GridStrategy, GridConfig, GridInstance, GridLevel, GridStatus, LevelStatus, Position
)

logger = logging.getLogger(__name__)


class GridTradeManager:
    """网格交易管理器"""

    def __init__(self, client: OKXClient):
        self.client = client
        self.strategy = GridStrategy()
        self.running = False
        # 订单查询偶发失败时避免立即重挂导致重复下单
        self._missing_order_checks: Dict[str, int] = {}

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

        if lower_price >= upper_price:
            return {"success": False, "message": "价格下限必须小于上限"}

        if grid_num < 2 or grid_num > 100:
            return {"success": False, "message": "网格数量必须在 2-100 之间"}

        if stop_loss_price and stop_loss_price >= lower_price:
            return {"success": False, "message": "止损价应小于网格下限"}

        if take_profit_price and take_profit_price <= upper_price:
            return {"success": False, "message": "止盈价应大于网格上限"}

        # 检查最小投资金额（每格至少 5 USDT）
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
        """启动网格（幂等）"""
        grid = self.strategy.get_grid(grid_id)
        if not grid:
            return {"success": False, "message": "网格不存在"}

        ticker = self.client.get_ticker(grid.config.inst_id)
        if not ticker:
            return {"success": False, "message": "无法获取行情"}

        current_price = Decimal(ticker.get('last', '0'))
        if current_price == 0:
            current_price = Decimal(ticker.get('bidPx', '0'))
        if current_price == 0:
            return {"success": False, "message": "行情价格异常"}

        already_active = (grid.status == GridStatus.ACTIVE)
        grid.status = GridStatus.ACTIVE

        logger.info(f"启动网格 {grid_id}, 当前价格：{current_price}")
        changed = await self._sync_grid_on_start(grid, current_price)
        if changed:
            self.strategy.update_grid(grid.grid_id)

        if already_active:
            return {"success": True, "message": "网格已在运行，已完成状态同步"}
        return {"success": True, "message": "网格已启动"}

    async def _sync_grid_on_start(self, grid: GridInstance, current_price: Decimal) -> bool:
        """
        启动时同步状态：
        - 校正异常状态
        - 先确保已有持仓的卖单
        - 再补挂买单
        """
        changed = False

        for level in grid.levels:
            if level.status == LevelStatus.CANCELLED:
                level.status = LevelStatus.PENDING
                level.order_id = None
                changed = True
            if level.status == LevelStatus.FILLED and not grid.get_position(level.level_id):
                level.status = LevelStatus.PENDING
                level.filled_price = None
                level.order_id = None
                changed = True

        # 先保证卖单
        for level_id in list(grid.positions.keys()):
            ensured = await self._ensure_sell_order_for_position(grid, level_id)
            changed = changed or ensured

        # 再补挂买单
        for level in grid.levels:
            placed = await self._check_and_place_buy_order(grid, level, current_price)
            changed = changed or placed

        return changed

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
        return {"success": False, "message": result.message}

    async def cancel_order(self, inst_id: str, order_id: str) -> bool:
        """撤销订单"""
        return self.client.cancel_order(inst_id, order_id)

    async def stop_grid(self, grid_id: str) -> Dict:
        """停止网格并清仓"""
        grid = self.strategy.get_grid(grid_id)
        if not grid:
            return {"success": False, "message": "网格不存在"}

        self.strategy.stop_grid(grid_id)
        logger.info(f"停止网格 {grid_id}")
        errors: List[str] = []
        locked_position_levels = set()

        # 撤销买单
        for level in grid.levels:
            if level.order_id and level.status == LevelStatus.ORDER_PLACED:
                order_id = level.order_id
                cancel_ok = await self.cancel_order(grid.config.inst_id, order_id)
                if cancel_ok:
                    reconciled = await self._reconcile_buy_order_after_cancel(
                        grid,
                        level,
                        order_id,
                        place_sell=False
                    )
                    if not reconciled:
                        level.order_id = None
                        level.status = LevelStatus.PENDING
                else:
                    if self.client.is_order_live(grid.config.inst_id, order_id):
                        errors.append(f"买单撤销失败 level={level.level_id} order_id={order_id}")
                    else:
                        # 交易所已不存在该订单，本地可清理
                        reconciled = await self._reconcile_buy_order_after_cancel(
                            grid,
                            level,
                            order_id,
                            place_sell=False
                        )
                        if not reconciled:
                            level.order_id = None
                            level.status = LevelStatus.PENDING
            else:
                level.order_id = None
                level.status = LevelStatus.FILLED if grid.get_position(level.level_id) else LevelStatus.PENDING
            level.order_type = "buy"

        # 撤销持仓卖单
        for position in list(grid.positions.values()):
            if position.sell_order_id:
                order_id = position.sell_order_id
                fallback_price = position.target_sell_price
                cancel_ok = await self.cancel_order(grid.config.inst_id, order_id)
                if cancel_ok:
                    await self._reconcile_sell_order_after_cancel(
                        grid,
                        position.level_id,
                        order_id,
                        fallback_price
                    )
                    remaining = grid.get_position(position.level_id)
                    if remaining:
                        remaining.sell_order_id = None
                else:
                    if self.client.is_order_live(grid.config.inst_id, order_id):
                        errors.append(
                            f"卖单撤销失败 level={position.level_id} order_id={order_id}"
                        )
                        locked_position_levels.add(position.level_id)
                    else:
                        await self._reconcile_sell_order_after_cancel(
                            grid,
                            position.level_id,
                            order_id,
                            fallback_price
                        )
                        remaining = grid.get_position(position.level_id)
                        if remaining:
                            remaining.sell_order_id = None

        if grid.positions:
            logger.info(f"网格 {grid_id} 有 {len(grid.positions)} 个持仓，开始平仓")
            close_errors = await self._close_all_positions(grid, skip_levels=locked_position_levels)
            errors.extend(close_errors)

        self.strategy.update_grid(grid.grid_id)
        if errors:
            logger.warning(f"网格已停止但有未完成项：{grid_id} - {'; '.join(errors)}")
            return {"success": False, "message": f"网格已停止，但存在未完成操作：{'；'.join(errors)}"}

        logger.info(f"网格已停止：{grid_id}")
        return {"success": True, "message": "网格已停止并完成撤单清仓"}

    def delete_grid(self, grid_id: str) -> Dict:
        """删除网格"""
        grid = self.strategy.get_grid(grid_id)
        if not grid:
            return {"success": False, "message": "网格不存在"}

        if grid.status == GridStatus.ACTIVE:
            return {"success": False, "message": "网格正在运行，请先停止再删除"}

        pending_buy_orders = [l for l in grid.levels if l.status == LevelStatus.ORDER_PLACED]
        pending_sell_orders = [p for p in grid.positions.values() if p.sell_order_id]
        if pending_buy_orders or pending_sell_orders:
            return {"success": False, "message": "网格还有挂单未撤销，请先停止网格"}

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
                    "target_sell": str(pos.target_sell_price),
                    "sell_order_id": pos.sell_order_id
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

                    ticker = self.client.get_ticker(grid.config.inst_id)
                    if not ticker:
                        continue

                    current_price = Decimal(ticker.get('last', '0'))
                    if current_price == 0:
                        continue

                    action = self.strategy.check_stop_loss_take_profit(grid.grid_id, current_price)
                    if action:
                        logger.info(f"网格 {grid.grid_id} 触发 {action}")
                        await self.stop_grid(grid.grid_id)
                        continue

                    await self._check_orders_and_trade(grid, current_price)

                await asyncio.sleep(2)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"网格监控错误：{e}")
                await asyncio.sleep(5)

    async def _check_orders_and_trade(self, grid: GridInstance, current_price: Decimal):
        """检查买卖订单状态并执行补挂"""
        changed = False

        # 1) 买单生命周期
        for level in grid.levels:
            if level.status == LevelStatus.ORDER_PLACED:
                changed = (await self._check_buy_order_filled(grid, level)) or changed
            elif level.status == LevelStatus.FILLED:
                if grid.get_position(level.level_id):
                    changed = (await self._ensure_sell_order_for_position(grid, level.level_id)) or changed
                else:
                    level.status = LevelStatus.PENDING
                    level.order_id = None
                    level.filled_price = None
                    changed = True
            elif level.status in (LevelStatus.PENDING, LevelStatus.CANCELLED):
                if level.status == LevelStatus.CANCELLED:
                    level.status = LevelStatus.PENDING
                    level.order_id = None
                    changed = True
                changed = (await self._check_and_place_buy_order(grid, level, current_price)) or changed

        # 2) 卖单生命周期（按持仓追踪）
        for position in list(grid.positions.values()):
            changed = (await self._check_position_sell_order(grid, position)) or changed

        if changed:
            self.strategy.update_grid(grid.grid_id)

    @staticmethod
    def _normalize_order_state(raw_state: str) -> str:
        """统一订单状态枚举，兼容数字和字符串状态"""
        state = (raw_state or "").lower()
        if state in ("3", "filled"):
            return "filled"
        if state in ("4", "canceled", "cancelled", "mmp_canceled"):
            return "canceled"
        if state in ("5", "partially_filled"):
            return "partially_filled"
        if state in ("1", "2", "live"):
            return "live"
        return "unknown"

    @staticmethod
    def _extract_fill_info(order: Dict, fallback_price: Decimal, fallback_size: Decimal) -> Tuple[Decimal, Decimal]:
        """提取成交均价和成交量"""
        filled_price = Decimal(order.get('avgPx') or order.get('fillPx') or str(fallback_price))
        filled_size = Decimal(order.get('accFillSz') or order.get('fillSz') or str(fallback_size))
        if filled_price <= 0:
            filled_price = fallback_price
        if filled_size <= 0:
            filled_size = fallback_size
        return filled_price, filled_size

    def _mark_order_seen(self, order_id: Optional[str]):
        if order_id:
            self._missing_order_checks.pop(order_id, None)

    def _mark_order_missing(self, order_id: Optional[str], threshold: int = 3) -> bool:
        """
        标记订单查询缺失；达到阈值后返回 True，表示可按失效处理。
        """
        if not order_id:
            return False
        count = self._missing_order_checks.get(order_id, 0) + 1
        self._missing_order_checks[order_id] = count
        return count >= threshold

    async def _fetch_final_order_snapshot(
        self,
        inst_id: str,
        order_id: str,
        retries: int = 4,
        interval: float = 0.25
    ) -> Optional[Dict]:
        """
        尝试在撤单后读取订单最终快照，用于识别部分成交。
        """
        final_order = await self._wait_order_status(
            inst_id,
            order_id,
            retries=retries,
            interval=interval
        )
        if final_order:
            return final_order
        return self.client.get_order_status(inst_id, order_id)

    async def _reconcile_buy_order_after_cancel(
        self,
        grid: GridInstance,
        level: GridLevel,
        order_id: str,
        place_sell: bool = False
    ) -> bool:
        """
        撤买单后对账：若存在部分成交，补记持仓。
        """
        order = await self._fetch_final_order_snapshot(grid.config.inst_id, order_id)
        if not order:
            return False

        filled_price, filled_size = self._extract_fill_info(order, level.price, Decimal("0"))
        if filled_size <= 0:
            return False

        await self._on_buy_filled(
            grid,
            level,
            filled_price,
            filled_size,
            place_sell=place_sell
        )
        return True

    async def _reconcile_sell_order_after_cancel(
        self,
        grid: GridInstance,
        buy_level_id: int,
        order_id: str,
        fallback_price: Decimal
    ) -> bool:
        """
        撤卖单后对账：若存在部分成交，补记利润和剩余仓位。
        """
        order = await self._fetch_final_order_snapshot(grid.config.inst_id, order_id)
        if not order:
            return False

        filled_price, filled_size = self._extract_fill_info(order, fallback_price, Decimal("0"))
        if filled_size <= 0:
            return False

        await self._on_sell_filled(grid, buy_level_id, filled_price, filled_size)
        return True

    async def _check_buy_order_filled(self, grid: GridInstance, level: GridLevel) -> bool:
        """检查买单是否成交"""
        if not level.order_id:
            level.status = LevelStatus.PENDING
            return True

        order = self.client.get_order_status(grid.config.inst_id, level.order_id)
        if not order:
            if self._mark_order_missing(level.order_id):
                if self.client.is_order_live(grid.config.inst_id, level.order_id):
                    logger.warning(
                        f"买单查询失败但订单仍 live，保持状态：{grid.config.inst_id} order_id={level.order_id}"
                    )
                    self._mark_order_seen(level.order_id)
                    return False
                logger.warning(f"买单连续查询失败且非 live，重置订单：{grid.config.inst_id} order_id={level.order_id}")
                order_id = level.order_id
                reconciled = await self._reconcile_buy_order_after_cancel(
                    grid,
                    level,
                    order_id,
                    place_sell=True
                )
                if not reconciled:
                    level.status = LevelStatus.PENDING
                    level.order_id = None
                return True
            return False

        self._mark_order_seen(level.order_id)

        state = self._normalize_order_state(order.get('state', ''))
        if state == "filled":
            filled_price, filled_size = self._extract_fill_info(order, level.price, level.size)
            await self._on_buy_filled(grid, level, filled_price, filled_size)
            return True
        if state == "canceled":
            filled_price, filled_size = self._extract_fill_info(order, level.price, Decimal("0"))
            if filled_size > 0:
                await self._on_buy_filled(grid, level, filled_price, filled_size)
            else:
                level.status = LevelStatus.PENDING
                level.order_id = None
            return True
        if state == "partially_filled":
            # 保持等待完全成交，不提前按满仓处理
            return False
        return False

    async def _on_buy_filled(
        self,
        grid: GridInstance,
        level: GridLevel,
        filled_price: Decimal,
        filled_size: Decimal,
        place_sell: bool = True
    ):
        """买单成交后：记录持仓并挂卖单"""
        target_sell_price = self.strategy.get_target_sell_price(grid, level.level_id)
        if target_sell_price is None:
            logger.error(f"买单成交但无法计算目标卖价：{grid.config.inst_id}, level={level.level_id}")
            level.status = LevelStatus.PENDING
            level.order_id = None
            return

        existing = grid.get_position(level.level_id)
        if existing:
            extra_size = filled_size
            if existing.sell_order_id:
                old_sell_order_id = existing.sell_order_id
                fallback_price = existing.target_sell_price
                cancel_ok = await self.cancel_order(grid.config.inst_id, old_sell_order_id)
                if cancel_ok:
                    await self._reconcile_sell_order_after_cancel(
                        grid,
                        level.level_id,
                        old_sell_order_id,
                        fallback_price
                    )
                elif self.client.is_order_live(grid.config.inst_id, old_sell_order_id):
                    # 无法安全重挂时，先用市价卖出新增仓位，避免未对冲暴露
                    logger.error(
                        f"旧卖单撤销失败且仍 live，执行紧急对冲：{grid.config.inst_id} "
                        f"level={level.level_id}, extra_size={extra_size}"
                    )
                    await self._emergency_hedge_extra_buy(grid, filled_price, extra_size)
                    level.status = LevelStatus.FILLED
                    level.order_id = None
                    level.filled_price = filled_price
                    level.order_type = "buy"
                    return
                else:
                    await self._reconcile_sell_order_after_cancel(
                        grid,
                        level.level_id,
                        old_sell_order_id,
                        fallback_price
                    )

                existing = grid.get_position(level.level_id)
                if existing and existing.sell_order_id == old_sell_order_id:
                    existing.sell_order_id = None

            existing = grid.get_position(level.level_id)
            if existing:
                new_size = existing.coin_size + filled_size
                weighted_price = (
                    existing.buy_price * existing.coin_size + filled_price * filled_size
                ) / new_size

                existing.coin_size = new_size
                existing.buy_price = weighted_price
                existing.target_sell_price = target_sell_price
            else:
                grid.add_position(
                    level_id=level.level_id,
                    coin_size=filled_size,
                    buy_price=filled_price,
                    target_sell_price=target_sell_price
                )
        else:
            grid.add_position(
                level_id=level.level_id,
                coin_size=filled_size,
                buy_price=filled_price,
                target_sell_price=target_sell_price
            )

        level.status = LevelStatus.FILLED
        level.order_id = None
        level.filled_price = filled_price
        level.order_type = "buy"

        logger.info(
            f"买单成交：{grid.config.inst_id} level={level.level_id} "
            f"@ {filled_price}, size={filled_size}, 目标卖出={target_sell_price}"
        )

        if place_sell:
            # 尝试挂卖单；失败不丢状态，后续循环会重试
            await self._ensure_sell_order_for_position(grid, level.level_id)

    async def _ensure_sell_order_for_position(self, grid: GridInstance, buy_level_id: int) -> bool:
        """确保该持仓对应的卖单已挂出"""
        position = grid.get_position(buy_level_id)
        if not position:
            return False

        if position.sell_order_id:
            return False

        result = await self._place_limit_order(
            grid.config.inst_id,
            "sell",
            str(position.coin_size),
            str(position.target_sell_price)
        )
        if result["success"]:
            position.sell_order_id = result["order_id"]
            logger.info(
                f"挂出卖单：{grid.config.inst_id} level={buy_level_id} "
                f"@ {position.target_sell_price}, size={position.coin_size}"
            )
            return True

        logger.warning(
            f"挂卖单失败：{grid.config.inst_id} level={buy_level_id} "
            f"@ {position.target_sell_price} - {result.get('message')}"
        )
        return False

    async def _check_position_sell_order(self, grid: GridInstance, position: Position) -> bool:
        """检查单个持仓对应的卖单状态"""
        if not position.sell_order_id:
            return await self._ensure_sell_order_for_position(grid, position.level_id)

        order = self.client.get_order_status(grid.config.inst_id, position.sell_order_id)
        if not order:
            if self._mark_order_missing(position.sell_order_id):
                if self.client.is_order_live(grid.config.inst_id, position.sell_order_id):
                    logger.warning(
                        f"卖单查询失败但订单仍 live，保持状态：{grid.config.inst_id} order_id={position.sell_order_id}"
                    )
                    self._mark_order_seen(position.sell_order_id)
                    return False
                logger.warning(
                    f"卖单连续查询失败且非 live，清理卖单引用：{grid.config.inst_id} order_id={position.sell_order_id}"
                )
                order_id = position.sell_order_id
                await self._reconcile_sell_order_after_cancel(
                    grid,
                    position.level_id,
                    order_id,
                    position.target_sell_price
                )
                remaining = grid.get_position(position.level_id)
                if remaining:
                    remaining.sell_order_id = None
                return True
            return False

        self._mark_order_seen(position.sell_order_id)

        state = self._normalize_order_state(order.get('state', ''))
        if state == "filled":
            filled_price, filled_size = self._extract_fill_info(order, position.target_sell_price, position.coin_size)
            await self._on_sell_filled(grid, position.level_id, filled_price, filled_size)
            return True
        if state == "canceled":
            filled_price, partial_size = self._extract_fill_info(order, position.target_sell_price, Decimal("0"))
            if partial_size > 0:
                await self._on_sell_filled(grid, position.level_id, filled_price, partial_size)
                remaining = grid.get_position(position.level_id)
                if remaining:
                    remaining.sell_order_id = None
            else:
                position.sell_order_id = None
            return True
        if state == "partially_filled":
            # 只在接近完全成交时按已完成处理
            _, partial_size = self._extract_fill_info(order, position.target_sell_price, position.coin_size)
            if partial_size >= position.coin_size * Decimal("0.999"):
                filled_price, filled_size = self._extract_fill_info(
                    order, position.target_sell_price, position.coin_size
                )
                await self._on_sell_filled(grid, position.level_id, filled_price, filled_size)
                return True
        return False

    async def _on_sell_filled(
        self,
        grid: GridInstance,
        buy_level_id: int,
        filled_price: Decimal,
        filled_size: Decimal
    ):
        """卖单成交后：计算利润并重置买单级别"""
        position = grid.get_position(buy_level_id)
        if not position:
            return

        close_size = position.coin_size if filled_size <= 0 else min(position.coin_size, filled_size)
        profit = (filled_price - position.buy_price) * close_size
        grid.total_profit += profit
        grid.total_trades += 1

        logger.info(
            f"卖单成交：{grid.config.inst_id} level={buy_level_id} "
            f"@ {filled_price}, size={close_size}, 利润={profit}"
        )

        remaining_size = position.coin_size - close_size
        if remaining_size > Decimal("0"):
            position.coin_size = remaining_size
            position.sell_order_id = None
            logger.info(
                f"卖单部分成交后保留剩余仓位：{grid.config.inst_id} "
                f"level={buy_level_id}, remaining={remaining_size}"
            )
            return

        grid.remove_position(buy_level_id)
        if 0 <= buy_level_id < len(grid.levels):
            buy_level = grid.levels[buy_level_id]
            buy_level.status = LevelStatus.PENDING
            buy_level.order_id = None
            buy_level.order_type = "buy"
            buy_level.filled_price = None

    async def _close_all_positions(self, grid: GridInstance, skip_levels: Optional[set] = None) -> List[str]:
        """平仓所有持仓（用于止损/止盈时）"""
        logger.info(f"开始平仓网格 {grid.grid_id} 的所有持仓")
        errors: List[str] = []
        skip_levels = skip_levels or set()
        for position in list(grid.positions.values()):
            if position.level_id in skip_levels:
                msg = f"跳过平仓（仍有 live 卖单） level={position.level_id} order_id={position.sell_order_id}"
                logger.warning(msg)
                errors.append(msg)
                continue
            try:
                result = await self._place_market_order(
                    grid.config.inst_id,
                    "sell",
                    str(position.coin_size)
                )
                if not result["success"]:
                    msg = f"平仓下单失败 level={position.level_id} - {result.get('message')}"
                    logger.warning(msg)
                    errors.append(msg)
                    continue

                order_id = result["order_id"]
                final_order = await self._wait_order_status(grid.config.inst_id, order_id)

                if not final_order and self.client.is_order_live(grid.config.inst_id, order_id):
                    position.sell_order_id = order_id
                    msg = f"平仓订单仍在成交中 level={position.level_id} order_id={order_id}"
                    logger.warning(msg)
                    errors.append(msg)
                    continue

                if final_order:
                    filled_price, filled_size = self._extract_fill_info(
                        final_order, position.buy_price, Decimal("0")
                    )
                    if filled_size > 0:
                        await self._on_sell_filled(grid, position.level_id, filled_price, filled_size)
                        # 被 _on_sell_filled 完全平仓后会自动移除
                        if grid.get_position(position.level_id):
                            msg = (
                                f"平仓部分成交，剩余仓位 level={position.level_id} "
                                f"remaining={grid.get_position(position.level_id).coin_size}"
                            )
                            logger.warning(msg)
                            errors.append(msg)
                    else:
                        msg = f"平仓订单无成交 level={position.level_id} order_id={order_id}"
                        logger.warning(msg)
                        errors.append(msg)
                        position.sell_order_id = order_id if self.client.is_order_live(grid.config.inst_id, order_id) else None
                else:
                    msg = f"平仓结果未知 level={position.level_id} order_id={order_id}"
                    logger.warning(msg)
                    errors.append(msg)
                    position.sell_order_id = order_id if self.client.is_order_live(grid.config.inst_id, order_id) else None
            except Exception as e:
                msg = f"平仓异常 level={position.level_id}: {e}"
                logger.error(msg)
                errors.append(msg)
        logger.info(f"平仓完成，最终利润：{grid.total_profit}")
        return errors

    async def _check_and_place_buy_order(self, grid: GridInstance, level: GridLevel, current_price: Decimal) -> bool:
        """检查并挂买单"""
        if level.status != LevelStatus.PENDING:
            return False

        if grid.get_position(level.level_id):
            # 该买单层已有持仓，等卖出后再重挂买单
            return False

        if level.order_id:
            level.status = LevelStatus.ORDER_PLACED
            return False

        # 买单应挂在当前价下方
        if current_price <= level.price:
            return False

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
            return True

        logger.warning(f"挂买单失败：{grid.config.inst_id} @ {level.price} - {result.get('message')}")
        return False

    async def _wait_order_status(
        self,
        inst_id: str,
        order_id: str,
        retries: int = 10,
        interval: float = 0.6
    ) -> Optional[Dict]:
        """轮询订单状态，等待到可确认状态"""
        for _ in range(retries):
            order = self.client.get_order_status(inst_id, order_id)
            if order:
                state = self._normalize_order_state(order.get('state', ''))
                if state in {"filled", "canceled"}:
                    return order
            await asyncio.sleep(interval)
        return None

    async def _emergency_hedge_extra_buy(
        self,
        grid: GridInstance,
        buy_price: Decimal,
        coin_size: Decimal
    ):
        """
        当旧卖单无法安全撤销时，对新增买入做紧急对冲，避免裸露仓位。
        """
        result = await self._place_market_order(
            grid.config.inst_id,
            "sell",
            str(coin_size)
        )
        if not result["success"]:
            logger.error(
                f"紧急对冲失败：{grid.config.inst_id}, size={coin_size}, msg={result.get('message')}"
            )
            return

        ticker = self.client.get_ticker(grid.config.inst_id) or {}
        hedge_price = Decimal(ticker.get("last", "0")) if ticker else Decimal("0")
        if hedge_price > 0:
            profit = (hedge_price - buy_price) * coin_size
            grid.total_profit += profit
            grid.total_trades += 1
            logger.warning(
                f"紧急对冲完成：{grid.config.inst_id} size={coin_size}, "
                f"buy={buy_price}, sell~={hedge_price}, pnl~={profit}"
            )
