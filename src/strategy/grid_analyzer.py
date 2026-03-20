"""
网格交易适用性分析器

分析市场是否适合网格交易：
- 波动率：中等波动最适合（太高风险大，太低没收益）
- 趋势强度：震荡市场最适合，强趋势不适合
- 价格位置：中间区域最适合
- 网格经济性：单格间距需覆盖手续费和滑点
"""
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal

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

    def analyze(
        self,
        inst_id: str,
        lower_price: Optional[Decimal] = None,
        upper_price: Optional[Decimal] = None,
        grid_num: Optional[int] = None,
        round_trip_fee_percent: Decimal = Decimal("0.20")
    ) -> AnalysisResult:
        """
        分析交易对是否适合网格交易

        Args:
            inst_id: 交易对
            lower_price/upper_price/grid_num: 可选，提供后会评估网格经济性
            round_trip_fee_percent: 双边交易成本百分比估算（默认 0.20%）
        """
        # 获取 K 线并统一为时间升序（旧 -> 新）
        klines_4h_raw = self.client.get_klines(inst_id, bar="4H", limit=100)
        klines_1h_raw = self.client.get_klines(inst_id, bar="1H", limit=80)
        klines_4h = self._normalize_klines(klines_4h_raw)
        klines_1h = self._normalize_klines(klines_1h_raw)

        if len(klines_4h) < 30:
            return AnalysisResult(
                suitable=False,
                score=0,
                signals={"error": "数据不足"},
                suggestion="无法获取足够的 K 线数据",
                risk_warning="数据不足，无法分析"
            )

        volatility_score, volatility_desc = self._calc_volatility(klines_4h)
        trend_score, trend_desc = self._calc_trend(klines_4h)
        price_score, price_desc = self._calc_price_position(klines_4h)
        short_vol_score, short_vol_desc = self._calc_short_volatility(klines_1h)

        has_grid_params = (
            lower_price is not None and
            upper_price is not None and
            grid_num is not None and
            grid_num > 0 and
            upper_price > lower_price
        )
        if has_grid_params:
            grid_econ_score, grid_econ_desc = self._calc_grid_economics(
                lower_price, upper_price, grid_num, round_trip_fee_percent
            )
        else:
            grid_econ_score, grid_econ_desc = 60, "未提供网格参数（经济性中性）"

        # 综合评分（加入网格经济性）
        total_score = int(
            volatility_score * 0.30 +
            trend_score * 0.30 +
            price_score * 0.15 +
            short_vol_score * 0.10 +
            grid_econ_score * 0.15
        )

        # 风险闸门：强趋势和网格经济性差时，压制总分
        if trend_score < 40:
            total_score = min(total_score, 55)
        if volatility_score < 35:
            total_score = min(total_score, 58)
        if has_grid_params and grid_econ_score < 50:
            total_score = min(total_score, 55)

        # 带网格参数时阈值更实用；否则提升阈值，避免“泛化适合”
        threshold = 65 if has_grid_params else 70
        suitable = (
            total_score >= threshold and
            trend_score >= 45 and
            volatility_score >= 40 and
            short_vol_score >= 40 and
            (not has_grid_params or grid_econ_score >= 60)
        )

        if suitable:
            suggestion = f"综合评分 {total_score}/100 - 适合网格交易"
        else:
            suggestion = f"综合评分 {total_score}/100 - 不建议网格交易"

        risk_warning = self._gen_risk_warning(
            trend_score=trend_score,
            short_vol_score=short_vol_score,
            vol_score=volatility_score,
            grid_econ_score=grid_econ_score if has_grid_params else None
        )

        return AnalysisResult(
            suitable=suitable,
            score=total_score,
            signals={
                "volatility": volatility_desc,
                "trend": trend_desc,
                "price_position": price_desc,
                "short_volatility": short_vol_desc,
                "grid_economics": grid_econ_desc
            },
            suggestion=suggestion,
            risk_warning=risk_warning
        )

    @staticmethod
    def _normalize_klines(klines: List[Dict]) -> List[Dict]:
        """统一 K 线顺序为时间升序，过滤异常值"""
        normalized = []
        for k in klines or []:
            try:
                ts = int(k["ts"])
                _ = Decimal(k["c"])
                normalized.append(k)
            except Exception:
                continue
        normalized.sort(key=lambda x: int(x["ts"]))
        return normalized

    def _calc_volatility(self, klines: List[Dict]) -> Tuple[int, str]:
        """
        计算波动率（基于 ATR）

        口径：
        - 先计算 4H 的 ATR(14)%
        - 再换算为周等效波动（便于沿用原评分区间）
        """
        atr_pct_4h = self._calc_atr_percent(klines, period=14)
        if atr_pct_4h is None:
            return 50, "数据不足"

        # 4H -> 周等效（每周约 42 根 4H K）
        weekly_vol = atr_pct_4h * (42 ** 0.5)

        if 4 <= weekly_vol <= 18:
            score = 100
        elif weekly_vol < 4:
            score = max(25, int(100 - (4 - weekly_vol) * 16))
        else:
            score = max(20, int(100 - (weekly_vol - 18) * 7))

        if weekly_vol < 3:
            desc = f"过低 ({weekly_vol:.1f}%/周)"
        elif weekly_vol < 4:
            desc = f"偏低 ({weekly_vol:.1f}%/周)"
        elif weekly_vol <= 18:
            desc = f"适中 ({weekly_vol:.1f}%/周)"
        elif weekly_vol <= 28:
            desc = f"偏高 ({weekly_vol:.1f}%/周)"
        else:
            desc = f"过高 ({weekly_vol:.1f}%/周)"

        return score, f"{desc} | ATR14(4H)={atr_pct_4h:.2f}%"

    @staticmethod
    def _calc_atr_percent(klines: List[Dict], period: int = 14) -> Optional[float]:
        """
        计算 ATR(%)：ATR / 最新收盘价 * 100
        """
        if len(klines) < period + 1:
            return None

        true_ranges: List[float] = []
        for i in range(1, len(klines)):
            try:
                high = float(klines[i]["h"])
                low = float(klines[i]["l"])
                prev_close = float(klines[i - 1]["c"])
            except Exception:
                continue

            if high <= 0 or low <= 0 or prev_close <= 0:
                continue

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            true_ranges.append(tr)

        if len(true_ranges) < period:
            return None

        atr = sum(true_ranges[-period:]) / period
        try:
            last_close = float(klines[-1]["c"])
        except Exception:
            return None
        if last_close <= 0:
            return None
        return atr / last_close * 100

    def _calc_trend(self, klines: List[Dict]) -> Tuple[int, str]:
        """
        计算趋势强度（RSI + MA 斜率）

        RSI 40-60 + 低斜率更适合网格。
        """
        if len(klines) < 30:
            return 50, "数据不足"

        rsi = self._calc_rsi(klines, 14)
        ma_slope = self._calc_ma_slope(klines, period=20, lookback=5)

        # RSI 评分
        if 45 <= rsi <= 55:
            rsi_score = 100
        elif 40 <= rsi <= 60:
            rsi_score = 85
        elif 35 <= rsi <= 65:
            rsi_score = 65
        else:
            rsi_score = 30

        # 均线斜率评分
        if abs(ma_slope) < 2:
            ma_score = 100
        elif abs(ma_slope) < 5:
            ma_score = 80
        elif abs(ma_slope) < 8:
            ma_score = 60
        else:
            ma_score = 30

        score = int(rsi_score * 0.6 + ma_score * 0.4)

        # RSI 极值优先判定为强趋势，避免被其他维度稀释
        if rsi >= 70 or rsi <= 30:
            score = min(score, 35)
        elif abs(ma_slope) >= 8:
            score = min(score, 45)

        if rsi >= 70:
            trend_desc = f"强上涨趋势 (RSI={rsi:.0f}, 斜率={ma_slope:.1f}%)"
        elif rsi <= 30:
            trend_desc = f"强下跌趋势 (RSI={rsi:.0f}, 斜率={ma_slope:.1f}%)"
        elif abs(ma_slope) >= 8:
            trend_desc = f"趋势明显 (斜率={ma_slope:.1f}%)"
        elif 40 <= rsi <= 60 and abs(ma_slope) < 3:
            trend_desc = f"震荡 (RSI={rsi:.0f})"
        else:
            trend_desc = f"弱趋势 (RSI={rsi:.0f}, 斜率={ma_slope:.1f}%)"

        return score, trend_desc

    def _calc_price_position(self, klines: List[Dict]) -> Tuple[int, str]:
        """
        计算当前价格在近 50 根区间的位置（中间区域更优）。
        """
        if len(klines) < 20:
            return 50, "数据不足"

        window = klines[-50:]
        high = float(max(k["h"] for k in window))
        low = float(min(k["l"] for k in window))
        current = float(window[-1]["c"])

        range_size = high - low
        if range_size <= 0:
            return 50, "无波动"

        position = (current - low) / range_size * 100

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

    def _calc_short_volatility(self, klines: List[Dict]) -> Tuple[int, str]:
        """
        计算短期波动率（1 小时 K 线），判断近期是否异常。
        """
        if len(klines) < 20:
            return 50, "数据不足"

        closes = [float(k["c"]) for k in klines]
        changes = []
        for i in range(1, len(closes)):
            if closes[i - 1] <= 0:
                continue
            changes.append(abs(closes[i] - closes[i - 1]) / closes[i - 1] * 100)

        if len(changes) < 12:
            return 50, "数据不足"

        recent = changes[-10:]
        history = changes[:-10]
        avg_recent_vol = sum(recent) / len(recent)
        avg_long_vol = (sum(history) / len(history)) if history else avg_recent_vol

        ratio = avg_recent_vol / avg_long_vol if avg_long_vol > 0 else 1

        if 0.6 <= ratio <= 1.8:
            score = 100
            desc = "正常"
        elif ratio < 0.6:
            score = 60
            desc = "异常平静"
        elif ratio > 3:
            score = 40
            desc = "异常波动"
        else:
            score = 75
            desc = "略有波动"

        return score, desc

    def _calc_grid_economics(
        self,
        lower_price: Decimal,
        upper_price: Decimal,
        grid_num: int,
        round_trip_fee_percent: Decimal
    ) -> Tuple[int, str]:
        """
        评估单格间距是否覆盖手续费/滑点。
        """
        if grid_num <= 0 or upper_price <= lower_price:
            return 30, "网格参数异常"

        grid_step = (upper_price - lower_price) / Decimal(str(grid_num))
        mid_price = (upper_price + lower_price) / Decimal("2")
        if mid_price <= 0:
            return 30, "网格参数异常"

        spacing_pct = float(grid_step / mid_price * Decimal("100"))
        net_edge_pct = spacing_pct - float(round_trip_fee_percent)

        if spacing_pct >= 2.0 and net_edge_pct >= 1.2:
            score = 100
        elif spacing_pct >= 1.2 and net_edge_pct >= 0.6:
            score = 80
        elif spacing_pct >= 0.8 and net_edge_pct >= 0.2:
            score = 60
        elif spacing_pct >= 0.5 and net_edge_pct > 0:
            score = 40
        else:
            score = 20

        desc = (
            f"每格间距≈{spacing_pct:.2f}%, 成本≈{float(round_trip_fee_percent):.2f}%, "
            f"净边际≈{net_edge_pct:.2f}%"
        )
        return score, desc

    def _calc_rsi(self, klines: List[Dict], period: int = 14) -> float:
        """计算 RSI 指标（基于时间升序序列）"""
        closes = [float(k["c"]) for k in klines]
        if len(closes) < period + 1:
            return 50.0

        window = closes[-(period + 1):]
        gains = []
        losses = []
        for i in range(1, len(window)):
            change = window[i] - window[i - 1]
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

    def _calc_ma_slope(self, klines: List[Dict], period: int = 20, lookback: int = 5) -> float:
        """计算均线斜率（百分比）"""
        closes = [float(k["c"]) for k in klines]
        if len(closes) < period + lookback:
            return 0.0

        current_ma = sum(closes[-period:]) / period
        prev_ma = sum(closes[-period - lookback:-lookback]) / period
        if prev_ma == 0:
            return 0.0
        slope = (current_ma - prev_ma) / prev_ma * 100
        return slope

    def _gen_risk_warning(
        self,
        trend_score: int,
        short_vol_score: int,
        vol_score: int,
        grid_econ_score: Optional[int] = None
    ) -> str:
        """生成风险提示"""
        warnings = []

        if trend_score < 40:
            warnings.append("强趋势行情，可能快速突破网格区间")
        if short_vol_score < 50:
            warnings.append("近期波动异常，注意风险")
        if vol_score < 40:
            warnings.append("波动率过低，网格收益可能不佳")
        if grid_econ_score is not None and grid_econ_score < 60:
            warnings.append("网格间距可能难以覆盖手续费和滑点")

        if not warnings:
            return "无重大风险"
        return "；".join(warnings)
