"""
OKX 套利交易机器人 - 主程序
"""
import asyncio
import logging
import uvicorn
from pathlib import Path

from src.utils.config import config
from src.web.app import app, init_bot

# 配置日志
log_dir = Path(__file__).parent / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            log_dir / 'bot.log',
            encoding='utf-8'
        )
    ]
)

logger = logging.getLogger(__name__)


def main():
    """主函数"""
    logger.info("正在启动 OKX 网格交易机器人...")

    # 初始化 bot
    if config.is_configured():
        init_bot(config)
        logger.info("✓ Bot 初始化完成")
    else:
        logger.warning("⚠️  尚未配置 OKX API，请在 config/.env 文件中配置")
        logger.warning("⚠️  机器人将以只读模式运行（仅查看，不交易）")

    # 启动 Web 服务
    logger.info(f"Web 面板地址：http://{config.web_host}:{config.web_port}")
    logger.info("按 Ctrl+C 停止服务")

    uvicorn.run(
        app,
        host=config.web_host,
        port=config.web_port,
        log_level="info"
    )


if __name__ == "__main__":
    main()
