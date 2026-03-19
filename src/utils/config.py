"""
配置管理
"""
import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional
from decimal import Decimal


class Config:
    """配置管理类"""

    def __init__(self):
        # 加载环境变量
        env_path = Path(__file__).parent.parent.parent / 'config' / '.env'
        load_dotenv(env_path)

        # OKX API 配置
        self.okx_api_key: str = os.getenv('OKX_API_KEY', '')
        self.okx_secret_key: str = os.getenv('OKX_SECRET_KEY', '')
        self.okx_passphrase: str = os.getenv('OKX_PASSPHRASE', '')
        self.okx_api_password: str = os.getenv('OKX_API_PASSWORD', '')

        # 交易配置
        self.stop_loss_percent: Decimal = Decimal(os.getenv('STOP_LOSS_PERCENT', '2'))
        self.take_profit_percent: Decimal = Decimal(os.getenv('TAKE_PROFIT_PERCENT', '3'))

        # 服务器配置
        self.web_host: str = os.getenv('WEB_HOST', '127.0.0.1')
        self.web_port: int = int(os.getenv('WEB_PORT', '8000'))

        # 套利配置
        self.min_spread: Decimal = Decimal(os.getenv('MIN_SPREAD', '0.3'))

        # 交易配置
        self.order_size_usdt: Decimal = Decimal(os.getenv('ORDER_SIZE_USDT', '100'))

    def validate(self) -> bool:
        """验证配置是否完整"""
        required = [
            self.okx_api_key,
            self.okx_secret_key,
            self.okx_passphrase
        ]
        return all(required)

    def is_configured(self) -> bool:
        """检查是否已配置"""
        return self.okx_api_key != '' and self.okx_api_key != 'your_api_key'


# 全局配置实例
config = Config()
