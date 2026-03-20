"""
Web API 服务 - 网格交易版本
"""
import logging
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from decimal import Decimal
from pathlib import Path

from ..api.okx_client import OKXClient
from ..trading.grid_manager import GridTradeManager
from ..strategy.grid_analyzer import GridAnalyzer
from ..utils.config import Config

logger = logging.getLogger(__name__)


class GridCreateRequest(BaseModel):
    """创建网格请求"""
    inst_id: str  # 交易对，如 BTC-USDT
    lower_price: float  # 价格下限
    upper_price: float  # 价格上限
    grid_num: int  # 网格数量
    investment_amount: float  # 投资金额 (USDT)
    stop_loss_price: Optional[float] = None  # 止损价
    take_profit_price: Optional[float] = None  # 止盈价


class GridPreviewRequest(BaseModel):
    """网格预览请求"""
    lower_price: float
    upper_price: float
    grid_num: int


class TradingBot:
    """交易机器人主类"""

    def __init__(self, config: Config):
        self.config = config
        self.client: Optional[OKXClient] = None
        self.grid_manager: Optional[GridTradeManager] = None
        self.analyzer: Optional[GridAnalyzer] = None
        self.running = False
        self._monitor_task: Optional[asyncio.Task] = None

    def initialize(self) -> bool:
        """初始化客户端和引擎"""
        if not self.config.validate():
            logger.error("API 配置不完整")
            return False

        try:
            self.client = OKXClient(
                api_key=self.config.okx_api_key,
                secret_key=self.config.okx_secret_key,
                passphrase=self.config.okx_passphrase,
                password=self.config.okx_api_password
            )

            self.grid_manager = GridTradeManager(self.client)
            self.analyzer = GridAnalyzer(self.client)

            logger.info("初始化成功")
            return True
        except Exception as e:
            logger.error(f"初始化失败：{e}")
            return False

    async def start(self):
        """启动交易机器人"""
        if self.running:
            return

        self.running = True
        if self.grid_manager:
            self.grid_manager.running = True
            self._monitor_task = asyncio.create_task(self.grid_manager.monitor_and_trade())
        logger.info("交易机器人已启动")

    async def stop(self):
        """停止交易机器人"""
        self.running = False
        if self.grid_manager:
            self.grid_manager.running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("交易机器人已停止")

    def get_status(self) -> Dict:
        """获取状态"""
        grids = self.grid_manager.get_all_grids() if self.grid_manager else []
        total_profit = sum(Decimal(g.get('total_profit', '0')) for g in grids)

        return {
            'running': self.running,
            'configured': self.config.is_configured(),
            'grid_count': len(grids),
            'total_profit': str(total_profit)
        }

    def get_grids(self) -> List[Dict]:
        """获取所有网格"""
        if not self.grid_manager:
            return []
        return self.grid_manager.get_all_grids()

    def get_grid_info(self, grid_id: str) -> Optional[Dict]:
        """获取网格详情"""
        if not self.grid_manager:
            return None
        return self.grid_manager.get_grid_info(grid_id)


# FastAPI 应用
app = FastAPI(title="OKX 网格交易机器人")

bot: Optional[TradingBot] = None
_config: Optional[Config] = None


def init_bot(config: Config):
    """初始化全局 bot 实例"""
    global bot, _config
    _config = config
    bot = TradingBot(config)
    if bot.initialize():
        return bot
    # 即使初始化失败也保留 bot 实例（用于显示界面）
    bot = TradingBot(config)
    return bot


def get_or_create_bot() -> TradingBot:
    """获取或创建 bot 实例"""
    global bot, _config
    if bot is None:
        if _config is None:
            _config = Config()
        bot = TradingBot(_config)
        bot.initialize()
    return bot


@app.get("/")
async def root():
    """首页"""
    return HTMLResponse(content=open(
        str(Path(__file__).parent.parent.parent / 'web' / 'index.html'),
        'r',
        encoding='utf-8'
    ).read())


@app.get("/api/status")
async def get_status():
    """获取状态"""
    b = get_or_create_bot()
    return b.get_status()


@app.get("/api/grids")
async def get_grids():
    """获取所有网格"""
    b = get_or_create_bot()
    return b.get_grids()


@app.post("/api/grids/preview")
async def preview_grid(req: GridPreviewRequest):
    """预览网格价格"""
    b = get_or_create_bot()
    if not b.grid_manager:
        raise HTTPException(status_code=500, detail="Not initialized")

    levels = b.grid_manager.calculate_preview(
        Decimal(str(req.lower_price)),
        Decimal(str(req.upper_price)),
        req.grid_num
    )
    return {"levels": levels}


@app.get("/api/grids/analyze")
async def analyze_grid(
    inst_id: str,
    lower_price: Optional[float] = None,
    upper_price: Optional[float] = None,
    grid_num: Optional[int] = None
):
    """分析交易对是否适合网格交易"""
    b = get_or_create_bot()
    if not b.analyzer:
        raise HTTPException(status_code=500, detail="Not initialized")

    result = b.analyzer.analyze(
        inst_id=inst_id,
        lower_price=Decimal(str(lower_price)) if lower_price is not None else None,
        upper_price=Decimal(str(upper_price)) if upper_price is not None else None,
        grid_num=grid_num
    )
    return {
        "suitable": result.suitable,
        "score": result.score,
        "signals": result.signals,
        "suggestion": result.suggestion,
        "risk_warning": result.risk_warning
    }


@app.get("/api/grids/{grid_id}")
async def get_grid_info(grid_id: str):
    """获取网格详情"""
    b = get_or_create_bot()
    info = b.get_grid_info(grid_id)
    if not info:
        raise HTTPException(status_code=404, detail="Grid not found")
    return info


@app.post("/api/grids/create")
async def create_grid(req: GridCreateRequest):
    """创建网格"""
    b = get_or_create_bot()
    if not b.grid_manager:
        raise HTTPException(status_code=500, detail="Not initialized")

    result = b.grid_manager.create_grid(
        inst_id=req.inst_id,
        lower_price=Decimal(str(req.lower_price)),
        upper_price=Decimal(str(req.upper_price)),
        grid_num=req.grid_num,
        investment_amount=Decimal(str(req.investment_amount)),
        stop_loss_price=Decimal(str(req.stop_loss_price)) if req.stop_loss_price else None,
        take_profit_price=Decimal(str(req.take_profit_price)) if req.take_profit_price else None
    )

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return result


@app.post("/api/grids/{grid_id}/start")
async def start_grid(grid_id: str):
    """启动网格"""
    b = get_or_create_bot()
    if not b.grid_manager:
        raise HTTPException(status_code=500, detail="Not initialized")

    result = await b.grid_manager.start_grid(grid_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    # 如果机器人还没启动，自动启动（确保监控循环运行）
    if not b.running:
        await b.start()
        logger.info(f"网格 {grid_id} 启动，机器人监控循环已自动启动")

    return result


@app.post("/api/grids/{grid_id}/stop")
async def stop_grid(grid_id: str):
    """停止网格"""
    b = get_or_create_bot()
    if not b.grid_manager:
        raise HTTPException(status_code=500, detail="Not initialized")

    result = await b.grid_manager.stop_grid(grid_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.delete("/api/grids/{grid_id}")
async def delete_grid(grid_id: str):
    """删除网格"""
    b = get_or_create_bot()
    if not b.grid_manager:
        raise HTTPException(status_code=500, detail="Not initialized")

    result = b.grid_manager.delete_grid(grid_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.post("/api/start")
async def start_bot():
    """启动机器人"""
    b = get_or_create_bot()
    if not b.client:
        raise HTTPException(status_code=500, detail="API not configured")
    await b.start()
    return {"status": "started"}


@app.post("/api/stop")
async def stop_bot():
    """停止机器人"""
    b = get_or_create_bot()
    await b.stop()
    return {"status": "stopped"}


@app.get("/api/tickers")
async def get_tickers():
    """获取热门交易对行情"""
    b = get_or_create_bot()
    if not b.client:
        raise HTTPException(status_code=500, detail="API not configured")

    pairs = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'XRP-USDT', 'ADA-USDT',
             'DOGE-USDT', 'DOT-USDT', 'MATIC-USDT', 'AVAX-USDT', 'LINK-USDT']

    tickers = {}
    for pair in pairs:
        ticker = b.client.get_ticker(pair)
        if ticker:
            tickers[pair] = {
                'last': ticker.get('last', '0'),
                'bid': ticker.get('bidPx', '0'),
                'ask': ticker.get('askPx', '0')
            }

    return tickers
