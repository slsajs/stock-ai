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
        # 신호 강도 기준 상향 조정 (100점 만점 중 80점 이상)
        self.min_signal_score = 80  # 기존 3/5에서 80/100으로 상향
        
        # 지표별 가중치 설정
        self.indicator_weights = {
            'rsi': 30,        # RSI: 30%
            'macd': 25,       # MACD: 25% 
            'bollinger': 20,  # 볼린저밴드: 20%
            'volume': 15,     # 거래량: 15%
            'trend': 10       # 추세: 10%
        }
        
        # 목표 수익률 설정 (수수료 고려)
        self.min_target_profit_rate = 0.008  # 0.8%
        
        # 시장 변동성 임계값
        self.volatility_threshold = 2.0  # 평소 2배
        
        # 거래량 임계값  
        self.volume_threshold = 1.5  # 20일 평균의 1.5배
        
    def calculate_buy_signal_score(self, price_data: List[float], volume_data: List[float]) -> Tuple[float, List[str]]:
        """매수 신호 종합 점수 (0-100점, 가중치 적용)"""
        total_score = 0.0
        signal_reasons = []
        
        if len(price_data) < 50:
            return 0.0, ["데이터 부족"]
        
        # 필수 조건: 거래량 최소 기준 체크
        if len(volume_data) >= 20:
            avg_volume = sum(volume_data[-20:]) / 20
            if volume_data[-1] < avg_volume * self.volume_threshold:
                return 0.0, [f"거래량 부족 ({volume_data[-1]/avg_volume:.1f}배, 최소 {self.volume_threshold}배 필요)"]
        
        try:
            # 1. RSI 신호 (30% 가중치)
            rsi_score = 0
            rsi = self.calculate_rsi(price_data[-14:])
            if rsi < 20:  # 매우 강한 과매도
                rsi_score = 100
                signal_reasons.append(f"RSI 매우강한과매도({rsi:.1f})")
            elif rsi < 25:  # 강한 과매도
                rsi_score = 80
                signal_reasons.append(f"RSI 강한과매도({rsi:.1f})")
            elif rsi < 30:  # 과매도
                rsi_score = 60
                signal_reasons.append(f"RSI 과매도({rsi:.1f})")
            elif rsi < 35:  # 약한 과매도
                rsi_score = 30
                signal_reasons.append(f"RSI 약한과매도({rsi:.1f})")
            total_score += rsi_score * self.indicator_weights['rsi'] / 100
                
            # 2. MACD 신호 (25% 가중치)
            macd_score = 0
            macd_line, signal_line = self.calculate_macd(price_data)
            macd_diff = macd_line - signal_line
            
            if macd_line > signal_line and macd_line > 0 and macd_diff > 0.5:
                macd_score = 100
                signal_reasons.append("MACD 강한골든크로스")
            elif macd_line > signal_line and macd_line > 0:
                macd_score = 80
                signal_reasons.append("MACD 골든크로스")
            elif macd_line > signal_line and macd_diff > 0.2:
                macd_score = 60
                signal_reasons.append("MACD 상승전환")
            elif macd_line > signal_line:
                macd_score = 40
                signal_reasons.append("MACD 약한상승")
            total_score += macd_score * self.indicator_weights['macd'] / 100
                
            # 3. 볼린저밴드 신호 (20% 가중치)
            bb_score = 0
            bb_lower, bb_upper = self.calculate_bollinger_bands(price_data)
            bb_position = (price_data[-1] - bb_lower) / (bb_upper - bb_lower)
            
            if bb_position <= 0.05:  # 하단 5% 이내
                bb_score = 100
                signal_reasons.append("볼밴 강한하단터치")
            elif bb_position <= 0.1:  # 하단 10% 이내
                bb_score = 80
                signal_reasons.append("볼밴 하단터치")
            elif bb_position <= 0.2:  # 하단 20% 이내
                bb_score = 50
                signal_reasons.append("볼밴 하단근접")
            total_score += bb_score * self.indicator_weights['bollinger'] / 100
                
            # 4. 거래량 신호 (15% 가중치)
            volume_score = 0
            if len(volume_data) >= 20:
                avg_volume = sum(volume_data[-20:]) / 20
                volume_ratio = volume_data[-1] / avg_volume
                
                if volume_ratio > 3.0:  # 3배 이상
                    volume_score = 100
                    signal_reasons.append(f"거래량 폭증({volume_ratio:.1f}배)")
                elif volume_ratio > 2.5:  # 2.5배 이상
                    volume_score = 80
                    signal_reasons.append(f"거래량 급증({volume_ratio:.1f}배)")
                elif volume_ratio > 2.0:  # 2배 이상
                    volume_score = 60
                    signal_reasons.append(f"거래량 증가({volume_ratio:.1f}배)")
                elif volume_ratio >= self.volume_threshold:  # 1.5배 이상
                    volume_score = 40
                    signal_reasons.append(f"거래량 양호({volume_ratio:.1f}배)")
            total_score += volume_score * self.indicator_weights['volume'] / 100
                
            # 5. 추세 신호 (10% 가중치)
            trend_score = 0
            if len(price_data) >= 20:
                ma5 = sum(price_data[-5:]) / 5
                ma10 = sum(price_data[-10:]) / 10
                ma20 = sum(price_data[-20:]) / 20
                
                if price_data[-1] > ma5 > ma10 > ma20:
                    trend_score = 100
                    signal_reasons.append("강한상승추세")
                elif price_data[-1] > ma5 > ma20:
                    trend_score = 70
                    signal_reasons.append("상승추세")
                elif ma5 > ma20:
                    trend_score = 40
                    signal_reasons.append("약한상승추세")
            total_score += trend_score * self.indicator_weights['trend'] / 100
                    
        except Exception as e:
            logger.error(f"매수 신호 점수 계산 실패: {e}")
            return 0.0, [f"계산 오류: {e}"]
            
        return total_score, signal_reasons
    
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
        """매수 여부 최종 판단 - 강화된 필터링"""
        try:
            # 1. 시장 변동성 필터 (2배 이상 변동성 시 거래 중단)
            market_volatility = self._calculate_market_volatility(price_data)
            if market_volatility > self.volatility_threshold:
                return False, f"시장 변동성 과도({market_volatility:.1f}배, 임계값:{self.volatility_threshold}배)"
            
            # 2. 기존 시장 상황 필터 강화
            if market_condition[0] in ["급락", "고변동성", "패닉"]:
                return False, f"시장상황 부적절: {market_condition[1]}"
            
            # 3. 목표 수익률 필터 (수수료 고려)
            expected_return = self._estimate_potential_return(price_data)
            if expected_return < self.min_target_profit_rate:
                return False, f"목표수익률 부족({expected_return*100:.2f}%, 최소{self.min_target_profit_rate*100:.1f}% 필요)"
                
            # 4. 신호 점수 계산 (가중치 적용)
            score, reasons = self.calculate_buy_signal_score(price_data, volume_data)
            
            if score >= self.min_signal_score:
                return True, f"매수신호(점수:{score:.1f}/100) - {', '.join(reasons)}"
            else:
                return False, f"신호부족(점수:{score:.1f}/100, 최소:{self.min_signal_score}) - {', '.join(reasons)}"
                
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
    
    def _calculate_market_volatility(self, price_data: List[float]) -> float:
        """시장 변동성 계산 (현재 변동성 vs 평균 변동성)"""
        try:
            if len(price_data) < 30:
                return 1.0  # 데이터 부족 시 정상으로 간주
            
            # 최근 5일 변동성
            recent_prices = price_data[-5:]
            recent_volatility = np.std(recent_prices) / np.mean(recent_prices)
            
            # 전체 기간 평균 변동성
            historical_prices = price_data[-30:-5]  # 과거 25일
            if len(historical_prices) < 10:
                return 1.0
            
            historical_volatility = np.std(historical_prices) / np.mean(historical_prices)
            
            # 현재 변동성이 평균 변동성의 몇 배인지 계산
            if historical_volatility == 0:
                return 1.0
                
            volatility_ratio = recent_volatility / historical_volatility
            return volatility_ratio
            
        except Exception as e:
            logger.error(f"시장 변동성 계산 실패: {e}")
            return 1.0
    
    def _estimate_potential_return(self, price_data: List[float]) -> float:
        """잠재적 수익률 추정 (기술적 분석 기반)"""
        try:
            if len(price_data) < 20:
                return 0.0
            
            current_price = price_data[-1]
            
            # 저항선과 지지선 계산
            recent_highs = [max(price_data[i:i+5]) for i in range(len(price_data)-5, len(price_data), 5)]
            recent_lows = [min(price_data[i:i+5]) for i in range(len(price_data)-5, len(price_data), 5)]
            
            if not recent_highs or not recent_lows:
                return 0.0
            
            # 평균 저항선 계산
            resistance_level = np.mean(recent_highs[-3:]) if len(recent_highs) >= 3 else max(recent_highs)
            
            # 볼린저밴드 상단을 목표가로 고려
            bb_lower, bb_upper = self.calculate_bollinger_bands(price_data)
            
            # 목표가는 저항선과 볼린저밴드 상단 중 더 보수적인 값
            target_price = min(resistance_level, bb_upper)
            
            # 잠재 수익률 계산
            potential_return = (target_price - current_price) / current_price
            
            # 수수료 차감 (왕복 0.27% 가정)
            fee_adjusted_return = potential_return - 0.0027
            
            return max(0.0, fee_adjusted_return)
            
        except Exception as e:
            logger.error(f"잠재 수익률 추정 실패: {e}")
            return 0.0
    
    def get_enhanced_analysis_summary(self, price_data: List[float], volume_data: List[float]) -> Dict:
        """강화된 분석 결과 요약"""
        try:
            # 각 지표별 점수 계산
            score, reasons = self.calculate_buy_signal_score(price_data, volume_data)
            
            # 개별 지표 분석
            rsi = self.calculate_rsi(price_data[-14:]) if len(price_data) >= 14 else 50
            macd_line, signal_line = self.calculate_macd(price_data)
            bb_lower, bb_upper = self.calculate_bollinger_bands(price_data)
            
            volatility = self._calculate_market_volatility(price_data)
            potential_return = self._estimate_potential_return(price_data)
            
            # 거래량 분석
            volume_ratio = 1.0
            if len(volume_data) >= 20:
                avg_volume = sum(volume_data[-20:]) / 20
                volume_ratio = volume_data[-1] / avg_volume if avg_volume > 0 else 1.0
            
            return {
                'overall_score': score,
                'signal_reasons': reasons,
                'indicators': {
                    'rsi': rsi,
                    'macd_line': macd_line,
                    'macd_signal': signal_line,
                    'bb_position': (price_data[-1] - bb_lower) / (bb_upper - bb_lower) if bb_upper > bb_lower else 0.5,
                    'volume_ratio': volume_ratio
                },
                'risk_metrics': {
                    'volatility_ratio': volatility,
                    'potential_return': potential_return * 100,  # 백분율
                    'meet_volume_threshold': volume_ratio >= self.volume_threshold,
                    'meet_volatility_threshold': volatility <= self.volatility_threshold,
                    'meet_return_threshold': potential_return >= self.min_target_profit_rate
                },
                'recommendation': 'BUY' if score >= self.min_signal_score else 'HOLD'
            }
            
        except Exception as e:
            logger.error(f"강화된 분석 요약 실패: {e}")
            return {'error': str(e)}