"""
다중 지표 필터링을 위한 EnhancedSignalAnalyzer 클래스
RSI, MACD, 볼린저밴드, 거래량, 추세 등을 종합하여 매매 신호 점수 계산
"""

import numpy as np
import pandas as pd
import logging
from typing import List, Tuple

logger = logging.getLogger(__name__)


class EnhancedSignalAnalyzer:
    def __init__(self):
        self.min_signal_score = 3  # 5점 만점에 3점 이상
        
    def calculate_buy_signal_score(self, price_data: List[float], volume_data: List[float]) -> Tuple[int, List[str]]:
        """매수 신호 종합 점수 (0-5점)"""
        signal_score = 0
        signal_reasons = []
        
        if len(price_data) < 50:
            return 0, ["데이터 부족"]
            
        try:
            # 1. RSI 신호 (1점)
            rsi = self.calculate_rsi(price_data[-14:])
            if rsi < 30:
                signal_score += 1
                signal_reasons.append(f"RSI 과매도({rsi:.1f})")
                
            # 2. MACD 신호 (1점)  
            macd_line, signal_line = self.calculate_macd(price_data)
            if macd_line > signal_line and macd_line > 0:
                signal_score += 1
                signal_reasons.append("MACD 골든크로스")
                
            # 3. 볼린저밴드 신호 (1점)
            bb_lower, bb_upper = self.calculate_bollinger_bands(price_data)
            if price_data[-1] <= bb_lower:
                signal_score += 1
                signal_reasons.append("볼린저밴드 하단 터치")
                
            # 4. 거래량 신호 (1점)
            if len(volume_data) >= 20:
                avg_volume = sum(volume_data[-20:]) / 20
                if volume_data[-1] > avg_volume * 2:
                    signal_score += 1
                    signal_reasons.append(f"거래량 급증({volume_data[-1]/avg_volume:.1f}배)")
                
            # 5. 추세 신호 (1점)
            if len(price_data) >= 20:
                ma5 = sum(price_data[-5:]) / 5
                ma20 = sum(price_data[-20:]) / 20
                if price_data[-1] > ma5 > ma20:
                    signal_score += 1
                    signal_reasons.append("상승추세 확인")
                    
        except Exception as e:
            logger.error(f"매수 신호 점수 계산 실패: {e}")
            return 0, [f"계산 오류: {e}"]
            
        return signal_score, signal_reasons
    
    def calculate_sell_signal_score(self, price_data: List[float], volume_data: List[float]) -> Tuple[int, List[str]]:
        """매도 신호 종합 점수 (0-5점)"""
        signal_score = 0
        signal_reasons = []
        
        if len(price_data) < 50:
            return 0, ["데이터 부족"]
            
        try:
            # 1. RSI 신호 (1점)
            rsi = self.calculate_rsi(price_data[-14:])
            if rsi > 70:
                signal_score += 1
                signal_reasons.append(f"RSI 과매수({rsi:.1f})")
                
            # 2. MACD 신호 (1점)
            macd_line, signal_line = self.calculate_macd(price_data)
            if macd_line < signal_line and macd_line < 0:
                signal_score += 1
                signal_reasons.append("MACD 데드크로스")
                
            # 3. 볼린저밴드 신호 (1점)
            bb_lower, bb_upper = self.calculate_bollinger_bands(price_data)
            if price_data[-1] >= bb_upper:
                signal_score += 1
                signal_reasons.append("볼린저밴드 상단 터치")
                
            # 4. 거래량 신호 (1점) - 거래량 감소시 매도 신호
            if len(volume_data) >= 20:
                avg_volume = sum(volume_data[-20:]) / 20
                if volume_data[-1] < avg_volume * 0.5:
                    signal_score += 1
                    signal_reasons.append(f"거래량 급감({volume_data[-1]/avg_volume:.1f}배)")
                
            # 5. 추세 신호 (1점)
            if len(price_data) >= 20:
                ma5 = sum(price_data[-5:]) / 5
                ma20 = sum(price_data[-20:]) / 20
                if price_data[-1] < ma5 < ma20:
                    signal_score += 1
                    signal_reasons.append("하락추세 확인")
                    
        except Exception as e:
            logger.error(f"매도 신호 점수 계산 실패: {e}")
            return 0, [f"계산 오류: {e}"]
            
        return signal_score, signal_reasons
    
    def should_buy(self, price_data: List[float], volume_data: List[float], market_condition: Tuple[str, str]) -> Tuple[bool, str]:
        """매수 여부 최종 판단"""
        try:
            # 시장 상황 필터
            if market_condition[0] in ["급락", "고변동성"]:
                return False, f"시장상황 부적절: {market_condition[1]}"
                
            # 신호 점수 계산
            score, reasons = self.calculate_buy_signal_score(price_data, volume_data)
            
            if score >= self.min_signal_score:
                return True, f"매수신호(점수:{score}/5) - {', '.join(reasons)}"
            else:
                return False, f"신호부족(점수:{score}/5) - {', '.join(reasons)}"
                
        except Exception as e:
            logger.error(f"매수 판단 실패: {e}")
            return False, f"판단 오류: {e}"
    
    def should_sell(self, price_data: List[float], volume_data: List[float], market_condition: Tuple[str, str]) -> Tuple[bool, str]:
        """매도 여부 최종 판단"""
        try:
            # 신호 점수 계산
            score, reasons = self.calculate_sell_signal_score(price_data, volume_data)
            
            if score >= self.min_signal_score:
                return True, f"매도신호(점수:{score}/5) - {', '.join(reasons)}"
            else:
                return False, f"신호부족(점수:{score}/5) - {', '.join(reasons)}"
                
        except Exception as e:
            logger.error(f"매도 판단 실패: {e}")
            return False, f"판단 오류: {e}"
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """RSI 계산"""
        try:
            if len(prices) < period + 1:
                return 50.0  # 기본값
                
            deltas = np.diff(prices)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            avg_gain = np.mean(gains[-period:])
            avg_loss = np.mean(losses[-period:])
            
            if avg_loss == 0:
                return 100.0
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return rsi
            
        except Exception as e:
            logger.error(f"RSI 계산 실패: {e}")
            return 50.0
    
    def calculate_macd(self, prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float]:
        """MACD 계산"""
        try:
            if len(prices) < slow + signal:
                return 0.0, 0.0
                
            prices_array = np.array(prices)
            
            # EMA 계산
            ema_fast = self._calculate_ema(prices_array, fast)
            ema_slow = self._calculate_ema(prices_array, slow)
            
            # 현재 MACD 라인 값
            macd_line = ema_fast[-1] - ema_slow[-1]
            
            # MACD 히스토리 계산 - 배열 크기 맞춤
            macd_history = ema_fast - ema_slow  # 전체 기간에 대해 MACD 계산
            
            # Signal 라인은 MACD 히스토리의 EMA
            if len(macd_history) >= signal:
                signal_line = self._calculate_ema(macd_history, signal)[-1]
            else:
                signal_line = macd_line  # 데이터가 부족하면 현재 MACD 라인 값 사용
            
            return macd_line, signal_line
            
        except Exception as e:
            logger.error(f"MACD 계산 실패: {e}")
            return 0.0, 0.0
    
    def calculate_bollinger_bands(self, prices: List[float], period: int = 20, std_dev: float = 2.0) -> Tuple[float, float]:
        """볼린저밴드 계산"""
        try:
            if len(prices) < period:
                current_price = prices[-1]
                return current_price * 0.95, current_price * 1.05  # 임시값
                
            recent_prices = prices[-period:]
            sma = np.mean(recent_prices)
            std = np.std(recent_prices)
            
            upper_band = sma + (std * std_dev)
            lower_band = sma - (std * std_dev)
            
            return lower_band, upper_band
            
        except Exception as e:
            logger.error(f"볼린저밴드 계산 실패: {e}")
            current_price = prices[-1] if prices else 0
            return current_price * 0.95, current_price * 1.05
    
    def _calculate_ema(self, prices: np.ndarray, period: int) -> np.ndarray:
        """지수이동평균 계산"""
        try:
            if len(prices) == 0:
                return np.array([])
            
            if len(prices) < period:
                # 데이터가 부족하면 단순 평균으로 대체
                return np.full_like(prices, np.mean(prices), dtype=float)
                
            alpha = 2.0 / (period + 1)
            ema = np.zeros_like(prices, dtype=float)
            
            # 첫 번째 값은 단순히 가격 값 사용
            ema[0] = prices[0]
            
            # EMA 계산
            for i in range(1, len(prices)):
                ema[i] = alpha * prices[i] + (1 - alpha) * ema[i-1]
                
            return ema
            
        except Exception as e:
            logger.error(f"EMA 계산 실패: {e}")
            # 안전한 기본값 반환
            if len(prices) > 0:
                return np.full_like(prices, prices[-1], dtype=float)
            else:
                return np.array([], dtype=float)
    
    def get_support_resistance_levels(self, prices: List[float], window: int = 20) -> Tuple[float, float]:
        """지지/저항 레벨 계산"""
        try:
            if len(prices) < window:
                current_price = prices[-1]
                return current_price * 0.9, current_price * 1.1
                
            recent_prices = prices[-window:]
            support = min(recent_prices)
            resistance = max(recent_prices)
            
            return support, resistance
            
        except Exception as e:
            logger.error(f"지지/저항 레벨 계산 실패: {e}")
            current_price = prices[-1] if prices else 0
            return current_price * 0.9, current_price * 1.1
    
    def analyze_price_pattern(self, prices: List[float]) -> str:
        """가격 패턴 분석"""
        try:
            if len(prices) < 10:
                return "데이터부족"
                
            recent_prices = prices[-10:]
            
            # 상승 패턴 체크
            if recent_prices[-1] > recent_prices[0] * 1.05:
                return "상승패턴"
            # 하락 패턴 체크    
            elif recent_prices[-1] < recent_prices[0] * 0.95:
                return "하락패턴"
            else:
                return "횡보패턴"
                
        except Exception as e:
            logger.error(f"가격 패턴 분석 실패: {e}")
            return "알수없음"