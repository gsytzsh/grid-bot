"""
OKX API 封装模块
"""
from okx import Trade, Account, MarketData, PublicData
from typing import Optional, Dict, List
from dataclasses import dataclass
from decimal import Decimal
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

    def get_ticker(self, inst_id: str) -> Optional[Dict]:
        """获取行情数据"""
        try:
            result = self.market_api.get_ticker(instId=inst_id)
            if result and len(result) > 0:
                return result[0]
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
            if result and result.get('details'):
                return result['details']
            return []
        except Exception as e:
            logger.error(f"获取余额失败：{e}")
            return []

    def get_positions(self) -> List[Position]:
        """获取持仓列表"""
        try:
            result = self.account_api.get_positions()
            positions = []
            if result:
                for pos in result:
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
            order_args = {
                'instId': inst_id,
                'side': side,
                'sz': size,
                'tdMode': 'cash',  # 现货交易
                'ordType': order_type,
            }

            if price and order_type == "limit":
                order_args['px'] = price

            result = self.trade_api.place_order(**order_args)

            if result and result.get('ordId'):
                logger.info(f"下单成功：{side} {inst_id} size={size} price={price}")
                return OrderResult(
                    success=True,
                    order_id=result['ordId'],
                    message="Order placed successfully"
                )
            else:
                error_msg = result.get('msg', 'Unknown error') if result else 'Empty response'
                logger.error(f"下单失败：{error_msg}")
                return OrderResult(
                    success=False,
                    message=error_msg
                )
        except Exception as e:
            logger.error(f"下单异常：{e}")
            return OrderResult(success=False, message=str(e))

    def cancel_order(self, inst_id: str, order_id: str) -> bool:
        """撤销订单"""
        try:
            result = self.trade_api.cancel_order(instId=inst_id, ordId=order_id)
            return result and result.get('ordId') == order_id
        except Exception as e:
            logger.error(f"撤单失败：{e}")
            return False

    def get_order_status(self, inst_id: str, order_id: str) -> Optional[Dict]:
        """获取订单状态"""
        try:
            result = self.trade_api.get_order(instId=inst_id, ordId=order_id)
            if result and len(result) > 0:
                return result[0]
            return None
        except Exception as e:
            logger.error(f"获取订单状态失败：{e}")
            return None

    def get_trading_pairs(self) -> List[Dict]:
        """获取所有交易对"""
        try:
            result = self.public_api.get_instruments(instType='SPOT')
            if result:
                return [
                    {
                        'inst_id': item['instId'],
                        'base_ccy': item['baseCcy'],
                        'quote_ccy': item['quoteCcy'],
                        'min_sz': item.get('minSz', '0'),
                        'tick_sz': item.get('tickSz', '0.01'),
                    }
                    for item in result
                ]
            return []
        except Exception as e:
            logger.error(f"获取交易对失败：{e}")
            return []
