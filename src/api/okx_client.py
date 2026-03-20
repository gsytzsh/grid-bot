"""
OKX API 封装模块
"""
from okx import Trade, Account, MarketData, PublicData
from typing import Optional, Dict, List
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
import logging

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    """订单结果"""
    success: bool
    order_id: Optional[str] = None
    message: str = ""


@dataclass
class Position:
    """持仓信息"""
    inst_id: str
    available: Decimal
    total: Decimal
    avg_price: Decimal = Decimal("0")


class OKXClient:
    """OKX API 客户端封装"""

    def __init__(self, api_key: str, secret_key: str, passphrase: str, password: str = None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.password = password or passphrase

        # 初始化 OKX 客户端
        self.trade_api = Trade.TradeAPI(
            api_key=api_key,
            api_secret_key=secret_key,
            passphrase=passphrase,
            flag='0'  # 0: 正式交易，1: 模拟盘
        )

        self.account_api = Account.AccountAPI(
            api_key=api_key,
            api_secret_key=secret_key,
            passphrase=passphrase,
            flag='0'
        )

        self.market_api = MarketData.MarketAPI(
            api_key=api_key,
            api_secret_key=secret_key,
            passphrase=passphrase,
            flag='0'
        )

        self.public_api = PublicData.PublicAPI(
            api_key=api_key,
            api_secret_key=secret_key,
            passphrase=passphrase,
            flag='0'
        )
        self._instrument_rules_cache: Dict[str, Dict] = {}

    @staticmethod
    def _extract_data_items(result) -> List[Dict]:
        """兼容 SDK 返回格式，统一提取 data 列表"""
        if isinstance(result, dict):
            data = result.get('data', [])
            return data if isinstance(data, list) else []
        if isinstance(result, list):
            return result
        return []

    @staticmethod
    def _decimal_to_str(value: Decimal) -> str:
        """Decimal 转字符串，避免科学计数法"""
        return format(value.normalize(), 'f') if value != 0 else "0"

    @staticmethod
    def _to_decimal(value, default: str = "0") -> Decimal:
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal(default)

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        """按步长向下取整"""
        if step <= 0:
            return value
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return units * step

    def _get_instrument_rules(self, inst_id: str) -> Optional[Dict]:
        """获取交易对精度规则（tickSz/minSz/lotSz）"""
        if inst_id in self._instrument_rules_cache:
            return self._instrument_rules_cache[inst_id]
        try:
            result = self.public_api.get_instruments(instType='SPOT', instId=inst_id)
            data = self._extract_data_items(result)
            if data:
                item = data[0]
                rules = {
                    "tick_sz": self._to_decimal(item.get("tickSz", "0")),
                    "min_sz": self._to_decimal(item.get("minSz", "0")),
                    "lot_sz": self._to_decimal(item.get("lotSz", item.get("minSz", "0")))
                }
                self._instrument_rules_cache[inst_id] = rules
                return rules
        except Exception as e:
            logger.warning(f"获取交易规则失败 {inst_id}: {e}")
        return None

    @staticmethod
    def _is_live_state(state: str) -> bool:
        normalized = str(state or "").lower()
        return normalized in {"live", "partially_filled", "1", "2", "5"}

    def get_live_orders(self, inst_id: str) -> List[Dict]:
        """获取某交易对 live 订单（含部分成交）"""
        try:
            result = self.trade_api.get_order_list(instType="SPOT", instId=inst_id)
            data = self._extract_data_items(result)
            live_orders = []
            for item in data:
                if item.get("instId") != inst_id:
                    continue
                if self._is_live_state(item.get("state", "")):
                    live_orders.append(item)
            return live_orders
        except Exception as e:
            logger.warning(f"获取 live 订单失败 {inst_id}: {e}")
            return []

    def is_order_live(self, inst_id: str, order_id: str) -> bool:
        """检查订单是否仍在交易所 live"""
        for order in self.get_live_orders(inst_id):
            if order.get("ordId") == order_id:
                return True
        return False

    def get_ticker(self, inst_id: str) -> Optional[Dict]:
        """获取行情数据"""
        try:
            result = self.market_api.get_ticker(instId=inst_id)
            data = self._extract_data_items(result)
            if data:
                ticker_data = data[0]
                logger.info(f"提取行情数据 [{inst_id}]: last={ticker_data.get('last')}")
                return ticker_data
            logger.warning(f"获取行情返回空值 {inst_id}: {result}")
            return None
        except Exception as e:
            logger.error(f"获取行情失败 {inst_id}: {e}")
            return None

    def get_bid_price(self, inst_id: str) -> Optional[Decimal]:
        """获取买一价"""
        ticker = self.get_ticker(inst_id)
        if ticker and ticker.get('bidPx'):
            return Decimal(ticker['bidPx'])
        return None

    def get_ask_price(self, inst_id: str) -> Optional[Decimal]:
        """获取卖一价"""
        ticker = self.get_ticker(inst_id)
        if ticker and ticker.get('askPx'):
            return Decimal(ticker['askPx'])
        return None

    def get_account_balance(self) -> List[Dict]:
        """获取账户余额"""
        try:
            result = self.account_api.get_account_balance()
            logger.debug(f"余额 API 返回：{result}")
            data = self._extract_data_items(result)
            if data:
                details = data[0].get('details', [])
                return details if isinstance(details, list) else []
            return []
        except Exception as e:
            logger.error(f"获取余额失败：{e}")
            return []

    def get_positions(self) -> List[Position]:
        """获取持仓列表"""
        try:
            result = self.account_api.get_positions()
            positions = []
            for pos in self._extract_data_items(result):
                if pos.get('pos') and Decimal(pos.get('pos', '0')) > 0:
                    positions.append(Position(
                        inst_id=pos['instId'],
                        available=Decimal(pos.get('availPos', '0')),
                        total=Decimal(pos.get('pos', '0')),
                        avg_price=Decimal(pos.get('avgPx', '0'))
                    ))
            return positions
        except Exception as e:
            logger.error(f"获取持仓失败：{e}")
            return []

    def place_order(
        self,
        inst_id: str,
        side: str,  # buy/sell
        size: str,
        price: Optional[str] = None,
        order_type: str = "limit"  # limit/market
    ) -> OrderResult:
        """下单交易"""
        try:
            size_dec = self._to_decimal(size)
            if size_dec <= 0:
                return OrderResult(success=False, message=f"订单数量无效: {size}")

            rules = self._get_instrument_rules(inst_id)
            if rules:
                lot_sz = rules.get("lot_sz", Decimal("0"))
                min_sz = rules.get("min_sz", Decimal("0"))
                if lot_sz > 0:
                    size_dec = self._floor_to_step(size_dec, lot_sz)
                elif min_sz > 0:
                    size_dec = self._floor_to_step(size_dec, min_sz)

                if min_sz > 0 and size_dec < min_sz:
                    return OrderResult(success=False, message=f"订单数量低于最小值 {min_sz}")

            order_args = {
                'instId': inst_id,
                'side': side,
                'sz': self._decimal_to_str(size_dec),
                'tdMode': 'cash',  # 现货交易
                'ordType': order_type,
            }

            if price and order_type == "limit":
                price_dec = self._to_decimal(price)
                if price_dec <= 0:
                    return OrderResult(success=False, message=f"订单价格无效: {price}")

                if rules:
                    tick_sz = rules.get("tick_sz", Decimal("0"))
                    if tick_sz > 0:
                        price_dec = self._floor_to_step(price_dec, tick_sz)

                if price_dec <= 0:
                    return OrderResult(success=False, message=f"归一化后价格无效: {price}")

                order_args['px'] = self._decimal_to_str(price_dec)

            result = self.trade_api.place_order(**order_args)
            data = self._extract_data_items(result)
            if isinstance(result, dict) and result.get('code') == '0' and data:
                item = data[0]
                if item.get('sCode', '0') == '0' and item.get('ordId'):
                    logger.info(f"下单成功：{side} {inst_id} size={size} price={price}")
                    return OrderResult(
                        success=True,
                        order_id=item['ordId'],
                        message=item.get('sMsg', 'Order placed successfully')
                    )
                error_msg = item.get('sMsg', 'Unknown error')
                logger.error(f"下单失败：{error_msg}")
                return OrderResult(success=False, message=error_msg)

            error_msg = result.get('msg', 'Unknown error') if isinstance(result, dict) else 'Empty response'
            logger.error(f"下单失败：{error_msg}")
            return OrderResult(success=False, message=error_msg)
        except Exception as e:
            logger.error(f"下单异常：{e}")
            return OrderResult(success=False, message=str(e))

    def cancel_order(self, inst_id: str, order_id: str) -> bool:
        """撤销订单"""
        try:
            result = self.trade_api.cancel_order(instId=inst_id, ordId=order_id)
            data = self._extract_data_items(result)
            if isinstance(result, dict) and result.get('code') == '0' and data:
                item = data[0]
                return item.get('sCode', '0') == '0' and item.get('ordId') == order_id
            return False
        except Exception as e:
            logger.error(f"撤单失败：{e}")
            return False

    def get_order_status(self, inst_id: str, order_id: str) -> Optional[Dict]:
        """获取订单状态"""
        try:
            result = self.trade_api.get_order(instId=inst_id, ordId=order_id)
            data = self._extract_data_items(result)
            if isinstance(result, dict) and result.get('code') == '0' and data:
                return data[0]
            return None
        except Exception as e:
            logger.error(f"获取订单状态失败：{e}")
            return None

    def get_trading_pairs(self) -> List[Dict]:
        """获取所有交易对"""
        try:
            result = self.public_api.get_instruments(instType='SPOT')
            data = self._extract_data_items(result)
            if data:
                return [
                    {
                        'inst_id': item['instId'],
                        'base_ccy': item['baseCcy'],
                        'quote_ccy': item['quoteCcy'],
                        'min_sz': item.get('minSz', '0'),
                        'lot_sz': item.get('lotSz', item.get('minSz', '0')),
                        'tick_sz': item.get('tickSz', '0.01'),
                    }
                    for item in data
                ]
            return []
        except Exception as e:
            logger.error(f"获取交易对失败：{e}")
            return []

    def get_klines(self, inst_id: str, bar: str = "1H", limit: int = 100) -> List[Dict]:
        """
        获取 K 线数据

        Args:
            inst_id: 交易对，如 BTC-USDT
            bar: K 线周期，如 1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D/1W/1M
            limit: 返回数量，最多 300

        返回：
            [{
                'ts': '1234567890',  # 时间戳
                'o': '60000',        # 开盘价
                'h': '61000',        # 最高价
                'l': '59000',        # 最低价
                'c': '60500',        # 收盘价
                'vol': '1000'        # 成交量
            }, ...]
        """
        try:
            # OKX SDK v0.4.1 使用 get_history_candlesticks 方法
            result = self.market_api.get_history_candlesticks(instId=inst_id, bar=bar, limit=str(limit))
            logger.info(f"K 线 API 返回 [{inst_id}]: {result}")
            if result and isinstance(result, dict) and result.get('code') == '0':
                data = result.get('data', [])
                logger.info(f"K 线数据 [{inst_id}] 解析成功，共 {len(data)} 条")
                return [
                    {
                        'ts': int(item[0]),
                        'o': Decimal(item[1]),
                        'h': Decimal(item[2]),
                        'l': Decimal(item[3]),
                        'c': Decimal(item[4]),
                        'vol': Decimal(item[5])
                    }
                    for item in data
                ]
            elif isinstance(result, list):
                logger.info(f"K 线数据 [{inst_id}] 返回 list，共 {len(result)} 条")
                return [
                    {
                        'ts': int(item[0]),
                        'o': Decimal(item[1]),
                        'h': Decimal(item[2]),
                        'l': Decimal(item[3]),
                        'c': Decimal(item[4]),
                        'vol': Decimal(item[5])
                    }
                    for item in result
                ]
            logger.warning(f"K 线 API 返回空值 [{inst_id}]: {result}")
            return []
        except Exception as e:
            logger.error(f"获取 K 线失败 [{inst_id}]: {e}")
            return []
