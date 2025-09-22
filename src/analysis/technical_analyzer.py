import logging
from typing import List, Dict, Optional
from collections import deque
import statistics

logger = logging.getLogger(__name__)

class TechnicalAnalyzer:
    """기술적 분석 지표 계산 클래스"""
    
    def __init__(self):
        pass
    
    def calculate_moving_average(self, prices: List[float], period: int = 5) -> Optional[float]:
        """이동평균선 계산"""
        if len(prices) < period:
            return None
        
        recent_prices = prices[-period:]
        return sum(recent_prices) / period
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        """RSI 지표 계산 (0~100)"""
        if len(prices) < period + 1:
            return None
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return None
        
        # 최근 period개의 평균 상승/하락폭
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return round(rsi, 2)
    
    def detect_volume_surge(self, current_volume: int, recent_volumes: List[int], surge_ratio: float = 2.0) -> bool:
        """거래량 급증 감지"""
        if len(recent_volumes) < 5:
            return False
        
        avg_volume = sum(recent_volumes[-20:]) / min(20, len(recent_volumes))
        
        if avg_volume == 0:
            return False
        
        return current_volume > avg_volume * surge_ratio
    
    def calculate_bollinger_bands(self, prices: List[float], period: int = 20, std_dev: float = 2.0) -> Dict[str, float]:
        """볼린저 밴드 계산"""
        if len(prices) < period:
            return {}
        
        recent_prices = prices[-period:]
        ma = sum(recent_prices) / period
        variance = sum((x - ma) ** 2 for x in recent_prices) / period
        std = variance ** 0.5
        
        upper_band = ma + (std * std_dev)
        lower_band = ma - (std * std_dev)
        
        return {
            'middle': round(ma, 2),
            'upper': round(upper_band, 2),
            'lower': round(lower_band, 2)
        }
    
    def calculate_price_change_rate(self, current_price: float, prev_price: float) -> float:
        """가격 변화율 계산 (%)"""
        if prev_price == 0:
            return 0.0
        
        return round(((current_price - prev_price) / prev_price) * 100, 2)
    
    def detect_price_breakout(self, current_price: float, recent_highs: List[float], recent_lows: List[float]) -> str:
        """가격 돌파 패턴 감지"""
        if not recent_highs or not recent_lows:
            return "none"
        
        recent_high = max(recent_highs[-10:]) if len(recent_highs) >= 10 else max(recent_highs)
        recent_low = min(recent_lows[-10:]) if len(recent_lows) >= 10 else min(recent_lows)
        
        # 상향 돌파
        if current_price > recent_high * 1.01:  # 1% 이상 돌파
            return "upward_breakout"
        
        # 하향 돌파
        if current_price < recent_low * 0.99:  # 1% 이상 하락
            return "downward_breakout"
        
        return "none"
    
    def is_oversold(self, rsi: float) -> bool:
        """과매도 구간 판단"""
        return rsi is not None and rsi < 30
    
    def is_overbought(self, rsi: float) -> bool:
        """과매수 구간 판단 - 기준 완화"""
        return rsi is not None and rsi > 85  # 70 → 85로 상향
    
    def calculate_momentum(self, prices: List[float], period: int = 5) -> Optional[float]:
        """모멘텀 지표 계산"""
        if len(prices) < period + 1:
            return None
        
        current_price = prices[-1]
        past_price = prices[-period-1]
        
        if past_price == 0:
            return None
        
        momentum = ((current_price - past_price) / past_price) * 100
        return round(momentum, 2)