"""
网格交易适用性分析器

分析市场是否适合网格交易：
- 波动率：中等波动最适合（太高风险大，太低没收益）
- 趋势强度：震荡市场最适合，强趋势不适合
- 价格位置：中间区域最适合
"""
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AnalysisResult:
    """分析结果"""
    suitable: bool  # 是否适合网格
    score: int  # 综合评分 0-100
    signals: Dict[str, str]  # 各维度信号
    suggestion: str  # 建议
    risk_warning: str  # 风险提示


class GridAnalyzer:
    """网格交易分析器"""

    def __init__(self, client):
        self.client = client

    def analyze(self, inst_id: str) -> AnalysisResult:
        """
        分析交易对是否适合网格交易

        使用 4 小时 K 线，分析最近 50-100 根数据
        """
        # 获取 4 小时 K 线（最近 100 根，约 16 天数据）
        klines_4h = self.client.get_klines(inst_id, bar="4H", limit=100)

        # 获取 1 小时 K 线（最近 50 根，约 2 天数据）
        klines_1h = self.client.get_klines(inst_id, bar="1H", limit=50)

        if not klines_4h or len(klines_4h) < 30:
            return AnalysisResult(
                suitable=False,
                score=0,
                signals={"error": "数据不足"},
                suggestion="无法获取足够的 K 线数据",
                risk_warning="数据不足，无法分析"
            )

        # 1. 计算波动率（使用 4 小时 K 线）
        volatility_score, volatility_desc = self._calc_volatility(klines_4h)

        # 2. 计算趋势强度（RSI + 均线）
        trend_score, trend_desc = self._calc_trend(klines_4h)

        # 3. 计算价格位置
        price_score, price_desc = self._calc_price_position(klines_4h)

        # 4. 计算短期波动（1 小时，判断近期是否有异常）
        short_vol_score, short_vol_desc = self._calc_short_volatility(klines_1h)

        # 综合评分
        total_score = int(
            volatility_score * 0.35 +
            trend_score * 0.35 +
            price_score * 0.2 +
            short_vol_score * 0.1
        )

        # 判断是否适合
        suitable = total_score >= 60

        # 生成建议
        if suitable:
            suggestion = f"综合评分 {total_score}/100 - 适合网格交易"
        else:
            suggestion = f"综合评分 {total_score}/100 - 不建议网格交易"

        # 风险提示
        risk_warning = self._gen_risk_warning(trend_score, short_vol_score, volatility_score)

        return AnalysisResult(
            suitable=suitable,
            score=total_score,
            signals={
                "volatility": volatility_desc,
                "trend": trend_desc,
                "price_position": price_desc,
                "short_volatility": short_vol_desc
            },
            suggestion=suggestion,
            risk_warning=risk_warning
        )

    def _calc_volatility(self, klines: List[Dict]) -> tuple:
        """
        计算波动率

        使用平均真实波幅（ATR）/ 价格 来衡量波动率
        适合网格：5%-15%（周波动）
        """
        if len(klines) < 14:
            return 50, "数据不足"

        # 计算每根 K 线的涨跌幅
        price_changes = []
        for i in range(1, len(klines)):
            prev_close = float(klines[i-1]['c'])
            curr_close = float(klines[i]['c'])
            change = abs(curr_close - prev_close) / prev_close * 100
            price_changes.append(change)

        # 平均波动率（4 小时）
        avg_vol = sum(price_changes) / len(price_changes)

        # 换算成周波动率（一周约 42 根 4 小时 K 线）
        weekly_vol = float(avg_vol) * (42 ** 0.5)

        # 评分：5%-15% 得满分
        if 5 <= weekly_vol <= 15:
            score = 100
        elif weekly_vol < 5:
            score = max(30, 100 - (5 - weekly_vol) * 15)
        else:
            score = max(20, 100 - (weekly_vol - 15) * 8)

        if weekly_vol < 3:
            desc = f"过低 ({weekly_vol:.1f}%/周)"
        elif weekly_vol < 5:
            desc = f"偏低 ({weekly_vol:.1f}%/周)"
        elif weekly_vol <= 15:
            desc = f"适中 ({weekly_vol:.1f}%/周)"
        elif weekly_vol <= 25:
            desc = f"偏高 ({weekly_vol:.1f}%/周)"
        else:
            desc = f"过高 ({weekly_vol:.1f}%/周)"

        return score, desc

    def _calc_trend(self, klines: List[Dict]) -> tuple:
        """
        计算趋势强度

        使用 RSI 和均线斜率
        RSI 40-60 = 震荡（适合网格）
        RSI > 70 或 < 30 = 强趋势（不适合）
        """
        if len(klines) < 14:
            return 50, "数据不足"

        # 计算 RSI(14)
        rsi = self._calc_rsi(klines, 14)

        # 计算均线斜率（20 周期）
        ma_slope = self._calc_ma_slope(klines, 20)

        # RSI 评分
        if 40 <= rsi <= 60:
            rsi_score = 100
            rsi_desc = "震荡"
        elif 30 <= rsi < 40 or 60 < rsi <= 70:
            rsi_score = 70
            rsi_desc = "偏弱趋势"
        else:
            rsi_score = 30
            rsi_desc = "强趋势"

        # 均线斜率评分
        if abs(ma_slope) < 5:
            ma_score = 100
            ma_desc = "横盘"
        elif abs(ma_slope) < 10:
            ma_score = 70
            ma_desc = "缓坡"
        else:
            ma_score = 30
            ma_desc = "陡坡"

        # 综合
        score = int(rsi_score * 0.6 + ma_score * 0.4)

        if rsi < 30:
            trend_desc = f"强下跌趋势 (RSI={rsi:.0f})"
        elif rsi > 70:
            trend_desc = f"强上涨趋势 (RSI={rsi:.0f})"
        elif abs(ma_slope) > 15:
            trend_desc = f"趋势明显 (斜率={ma_slope:.1f}%)"
        elif 40 <= rsi <= 60 and abs(ma_slope) < 5:
            trend_desc = f"震荡 (RSI={rsi:.0f})"
        else:
            trend_desc = f"弱趋势 (RSI={rsi:.0f})"

        return score, trend_desc

    def _calc_price_position(self, klines: List[Dict]) -> tuple:
        """
        计算当前价格在区间中的位置

        使用最近 50 根 K 线的最高/最低价
        中间区域（30%-70%）最适合网格
        """
        if len(klines) < 20:
            return 50, "数据不足"

        # 最近 50 根的最高/最低价
        high = float(max(k['h'] for k in klines[:50]))
        low = float(min(k['l'] for k in klines[:50]))
        current = float(klines[0]['c'])

        # 计算位置百分比
        range_size = high - low
        if range_size == 0:
            return 50, "无波动"

        position = (current - low) / range_size * 100

        # 评分：30%-70% 得满分
        if 30 <= position <= 70:
            score = 100
        elif 20 <= position < 30 or 70 < position <= 80:
            score = 70
        else:
            score = 30

        if position < 10:
            desc = f"接近最低点 ({position:.0f}%)"
        elif position < 30:
            desc = f"偏低区域 ({position:.0f}%)"
        elif position <= 70:
            desc = f"中间区域 ({position:.0f}%)"
        elif position <= 90:
            desc = f"偏高区域 ({position:.0f}%)"
        else:
            desc = f"接近最高点 ({position:.0f}%)"

        return score, desc

    def _calc_short_volatility(self, klines: List[Dict]) -> tuple:
        """
        计算短期波动率（1 小时 K 线）

        判断近期是否有异常波动
        """
        if len(klines) < 10:
            return 50, "数据不足"

        # 计算最近 10 根的平均波动
        recent_changes = []
        for i in range(1, min(10, len(klines))):
            prev_close = float(klines[i-1]['c'])
            curr_close = float(klines[i]['c'])
            change = abs(curr_close - prev_close) / prev_close * 100
            recent_changes.append(change)

        avg_recent_vol = sum(recent_changes) / len(recent_changes)

        # 与长期波动比较
        long_changes = []
        for i in range(10, len(klines)):
            prev_close = float(klines[i-1]['c'])
            curr_close = float(klines[i]['c'])
            change = abs(curr_close - prev_close) / prev_close * 100
            long_changes.append(change)

        avg_long_vol = sum(long_changes) / len(long_changes) if long_changes else avg_recent_vol

        ratio = avg_recent_vol / avg_long_vol if avg_long_vol > 0 else 1

        # 评分
        if 0.5 <= ratio <= 2:
            score = 100
            desc = "正常"
        elif ratio < 0.5:
            score = 60
            desc = "异常平静"
        elif ratio > 3:
            score = 40
            desc = "异常波动"
        else:
            score = 80
            desc = "略有波动"

        return score, desc

    def _calc_rsi(self, klines: List[Dict], period: int = 14) -> float:
        """计算 RSI 指标"""
        if len(klines) < period + 1:
            return 50.0

        gains = []
        losses = []

        for i in range(1, min(period + 1, len(klines))):
            change = float(klines[i]['c']) - float(klines[i-1]['c'])
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(change))

        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

        return rsi

    def _calc_ma_slope(self, klines: List[Dict], period: int = 20) -> float:
        """计算均线斜率（百分比）"""
        if len(klines) < period * 2:
            return 0.0

        # 当前 MA
        current_ma = sum(float(k['c']) for k in klines[:period]) / period
        # 5 根前的 MA
        prev_ma = sum(float(k['c']) for k in klines[5:5+period]) / period

        if prev_ma == 0:
            return 0.0

        slope = (current_ma - prev_ma) / prev_ma * 100
        return slope

    def _gen_risk_warning(self, trend_score: int, short_vol_score: int, vol_score: int) -> str:
        """生成风险提示"""
        warnings = []

        if trend_score < 40:
            warnings.append("强趋势行情，可能快速突破网格区间")

        if short_vol_score < 50:
            warnings.append("近期波动异常，注意风险")

        if vol_score < 40:
            warnings.append("波动率过低，网格收益可能不佳")

        if not warnings:
            return "无重大风险"

        return "；".join(warnings)
