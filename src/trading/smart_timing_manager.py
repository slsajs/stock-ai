#!/usr/bin/env python3
"""
스마트 매수 타이밍 매니저
장 초반 급등 조정 대기, 높은 변동성 시점 회피 등 타이밍 최적화
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, time, timedelta
from dataclasses import dataclass

@dataclass 
class TimingCondition:
    """타이밍 조건"""
    is_trading_allowed: bool
    reason: str
    wait_minutes: Optional[int] = None
    risk_level: str = "LOW"  # LOW, MEDIUM, HIGH

class SmartTimingManager:
    """스마트 매수 타이밍 매니저"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
        # 타이밍 설정
        timing_config = self.config.get('smart_timing', {})

        # 장 초반 대기 설정 - 더 보수적으로 조정
        self.morning_wait_minutes = timing_config.get('morning_wait_minutes', 45)  # 45분으로 증가
        self.avoid_opening_surge = timing_config.get('avoid_opening_surge', True)

        # 변동성 기준 - 더 엄격하게 설정
        self.high_volatility_threshold = timing_config.get('high_volatility_threshold', 20.0)  # 25 → 20으로 강화
        self.extreme_volatility_threshold = timing_config.get('extreme_volatility_threshold', 30.0)  # 35 → 30으로 강화

        # 급등 회피 설정 - 더 보수적으로 설정
        self.max_surge_change = timing_config.get('max_surge_change', 12.0)  # config.json에서 설정
        self.volume_spike_threshold = timing_config.get('volume_spike_threshold', 3.5)  # config.json에서 설정

        # 거래량 폭증 쿨다운 설정
        self.volume_cooldown_minutes = timing_config.get('volume_cooldown_minutes', 10)  # 10분 쿨다운
        self.volume_spike_history = {}  # 종목별 거래량 폭증 기록

        # 시간별 포지션 관리 설정 추가
        self.min_holding_minutes = timing_config.get('min_holding_minutes', 45)  # 최소 홀딩 시간 45분
        self.profit_taking_time_minutes = timing_config.get('profit_taking_time_minutes', 15)  # 15분 후 수익시 매도 고려

        # 시장 상황 기준
        self.bearish_threshold = timing_config.get('bearish_threshold', -1.5)
        self.crash_threshold = timing_config.get('crash_threshold', -2.5)
        
        self.logger.info(f"Smart Timing Manager 초기화:")
        self.logger.info(f"  • 장초반 대기: {self.morning_wait_minutes}분")
        self.logger.info(f"  • 높은 변동성 임계값: {self.high_volatility_threshold}")
        self.logger.info(f"  • 급등 회피 기준: {self.max_surge_change}%")
    
    def check_trading_timing(self, market_data: Dict, stock_data: Dict = None) -> TimingCondition:
        """매수 타이밍 체크"""
        current_time = datetime.now().time()
        
        # 1. 장 시간 체크
        if not self._is_trading_hours(current_time):
            return TimingCondition(
                is_trading_allowed=False,
                reason="장외 시간",
                risk_level="HIGH"
            )
        
        # 2. 장 초반 급등 회피 체크
        opening_check = self._check_opening_timing(current_time)
        if not opening_check.is_trading_allowed:
            return opening_check
        
        # 3. 시장 변동성 체크  
        volatility_check = self._check_market_volatility(market_data)
        if not volatility_check.is_trading_allowed:
            return volatility_check
        
        # 4. 시장 상황 체크
        market_condition_check = self._check_market_condition(market_data)
        if not market_condition_check.is_trading_allowed:
            return market_condition_check
        
        # 5. 개별 종목 급등 체크 (종목 데이터가 있는 경우)
        if stock_data:
            surge_check = self._check_stock_surge(stock_data)
            if not surge_check.is_trading_allowed:
                return surge_check
        
        # 모든 조건 통과
        return TimingCondition(
            is_trading_allowed=True,
            reason="최적 매수 타이밍",
            risk_level="LOW"
        )
    
    def _is_trading_hours(self, current_time: time) -> bool:
        """거래 시간 체크"""
        trading_start = time(9, 0)  # 09:00
        trading_end = time(15, 30)  # 15:30
        
        return trading_start <= current_time <= trading_end
    
    def _check_opening_timing(self, current_time: time) -> TimingCondition:
        """장 초반 타이밍 체크"""
        if not self.avoid_opening_surge:
            return TimingCondition(is_trading_allowed=True, reason="장초반 체크 통과")
        
        # 장 시작 후 지정 시간까지 대기
        market_open = time(9, 0)
        wait_until = (datetime.combine(datetime.today(), market_open) + 
                     timedelta(minutes=self.morning_wait_minutes)).time()
        
        if current_time < wait_until:
            remaining_minutes = int((datetime.combine(datetime.today(), wait_until) - 
                                   datetime.combine(datetime.today(), current_time)).total_seconds() / 60)
            
            return TimingCondition(
                is_trading_allowed=False,
                reason=f"장초반 급등 조정 대기 (남은시간: {remaining_minutes}분)",
                wait_minutes=remaining_minutes,
                risk_level="MEDIUM"
            )
        
        return TimingCondition(is_trading_allowed=True, reason="장초반 대기시간 완료")
    
    def _check_market_volatility(self, market_data: Dict) -> TimingCondition:
        """시장 변동성 체크"""
        volatility = market_data.get('volatility', 0)
        
        if volatility >= self.extreme_volatility_threshold:
            return TimingCondition(
                is_trading_allowed=False,
                reason=f"극심한 변동성 ({volatility:.1f} >= {self.extreme_volatility_threshold})",
                wait_minutes=15,
                risk_level="HIGH"
            )
        elif volatility >= self.high_volatility_threshold:
            return TimingCondition(
                is_trading_allowed=False,
                reason=f"높은 변동성 ({volatility:.1f} >= {self.high_volatility_threshold})",
                wait_minutes=10,
                risk_level="MEDIUM"
            )
        
        return TimingCondition(is_trading_allowed=True, reason="변동성 정상")
    
    def _check_market_condition(self, market_data: Dict) -> TimingCondition:
        """시장 상황 체크"""
        kospi_change = market_data.get('kospi_change', 0)
        kosdaq_change = market_data.get('kosdaq_change', 0)
        
        # 평균 시장 변화율
        avg_market_change = (kospi_change + kosdaq_change) / 2
        
        if avg_market_change <= self.crash_threshold:
            return TimingCondition(
                is_trading_allowed=False,
                reason=f"시장 급락 ({avg_market_change:.2f}% <= {self.crash_threshold}%)",
                wait_minutes=30,
                risk_level="HIGH"
            )
        elif avg_market_change <= self.bearish_threshold:
            return TimingCondition(
                is_trading_allowed=False,
                reason=f"시장 약세 ({avg_market_change:.2f}% <= {self.bearish_threshold}%)",
                wait_minutes=15,
                risk_level="MEDIUM"
            )
        
        return TimingCondition(is_trading_allowed=True, reason="시장 상황 양호")
    
    def _check_stock_surge(self, stock_data: Dict) -> TimingCondition:
        """개별 종목 급등 체크"""
        daily_change = abs(stock_data.get('daily_change_pct', 0))
        volume_ratio = stock_data.get('volume_ratio', 1.0)
        
        # 급등 체크
        if daily_change >= self.max_surge_change:
            return TimingCondition(
                is_trading_allowed=False,
                reason=f"급등주 회피 (일일변동: {daily_change:.2f}%)",
                risk_level="HIGH"
            )
        
        # 거래량 급증 체크 (강화된 로직)
        current_time = datetime.now()

        # 거래량 폭증 기록 확인
        if stock_code in self.volume_spike_history:
            last_spike_time = self.volume_spike_history[stock_code]
            time_since_spike = (current_time - last_spike_time).total_seconds() / 60

            if time_since_spike < self.volume_cooldown_minutes:
                return TimingCondition(
                    is_trading_allowed=False,
                    reason=f"거래량 폭증 쿨다운 중 (남은시간: {self.volume_cooldown_minutes - time_since_spike:.0f}분)",
                    risk_level="MEDIUM"
                )

        # 현재 거래량 급증 체크
        if volume_ratio >= self.volume_spike_threshold:
            # 거래량 폭증 기록
            self.volume_spike_history[stock_code] = current_time

            return TimingCondition(
                is_trading_allowed=False,
                reason=f"거래량 급증 회피 (거래량: {volume_ratio:.1f}배, {self.volume_cooldown_minutes}분 대기)",
                risk_level="HIGH"
            )

        # 중간 수준 거래량 증가도 경고
        elif volume_ratio >= 2.5:
            return TimingCondition(
                is_trading_allowed=True,
                reason=f"거래량 증가 주의 (거래량: {volume_ratio:.1f}배)",
                risk_level="MEDIUM"
            )
        
        return TimingCondition(is_trading_allowed=True, reason="종목 상태 정상")
    
    def get_optimal_entry_timing(self, market_data: Dict, stock_data: Dict = None) -> Dict:
        """최적 진입 타이밍 추천"""
        timing_condition = self.check_trading_timing(market_data, stock_data)
        
        result = {
            'is_optimal': timing_condition.is_trading_allowed,
            'reason': timing_condition.reason,
            'risk_level': timing_condition.risk_level,
            'recommended_action': 'BUY' if timing_condition.is_trading_allowed else 'WAIT'
        }
        
        if timing_condition.wait_minutes:
            result['wait_minutes'] = timing_condition.wait_minutes
            result['next_check_time'] = (datetime.now() + 
                                       timedelta(minutes=timing_condition.wait_minutes)).strftime('%H:%M')
        
        # 추가 권장사항
        if not timing_condition.is_trading_allowed:
            result['recommendations'] = self._get_timing_recommendations(timing_condition)
        
        return result
    
    def _get_timing_recommendations(self, condition: TimingCondition) -> List[str]:
        """타이밍 개선 권장사항"""
        recommendations = []
        
        if "장초반" in condition.reason:
            recommendations.append("장 안정화 대기 후 재진입 고려")
            recommendations.append("급등 종목보다 안정적 종목 우선 검토")
            
        elif "변동성" in condition.reason:
            recommendations.append("변동성 감소까지 관망")
            recommendations.append("소액 분할 매수 고려")
            
        elif "급락" in condition.reason or "약세" in condition.reason:
            recommendations.append("시장 반등 신호 확인까지 대기")
            recommendations.append("방어적 포트폴리오 유지")
            
        elif "급등" in condition.reason:
            recommendations.append("조정 구간 진입시 재검토")
            recommendations.append("다른 저평가 종목 발굴")
        
        return recommendations

    def check_position_timing(self, stock_code: str, entry_time: datetime, current_price: float, entry_price: float) -> TimingCondition:
        """보유 포지션의 매도 타이밍 체크"""
        try:
            hold_duration = (datetime.now() - entry_time).total_seconds() / 60  # 분 단위
            profit_rate = (current_price - entry_price) / entry_price * 100

            # 최대 보유시간 초과 체크
            if hold_duration >= self.max_position_hold_minutes:
                if profit_rate > 0:
                    return TimingCondition(
                        is_trading_allowed=True,  # 매도 허용
                        reason=f"시간만료 익절 ({self.max_position_hold_minutes}분 초과)",
                        risk_level="LOW"
                    )
                else:
                    return TimingCondition(
                        is_trading_allowed=True,  # 매도 허용
                        reason=f"시간만료 손절 ({self.max_position_hold_minutes}분 초과)",
                        risk_level="MEDIUM"
                    )

            # 수익 실현 타이밍 체크
            if hold_duration >= self.profit_taking_time_minutes and profit_rate >= 1.0:
                return TimingCondition(
                    is_trading_allowed=True,  # 매도 고려
                    reason=f"수익 실현 타이밍 ({self.profit_taking_time_minutes}분 경과, {profit_rate:+.2f}%)",
                    risk_level="LOW"
                )

            # 아직 보유 유지
            remaining_minutes = max(0, self.max_position_hold_minutes - hold_duration)
            return TimingCondition(
                is_trading_allowed=False,  # 보유 지속
                reason=f"보유 지속 (잔여시간: {remaining_minutes:.0f}분, 수익률: {profit_rate:+.2f}%)",
                wait_minutes=int(remaining_minutes),
                risk_level="LOW"
            )

        except Exception as e:
            self.logger.error(f"포지션 타이밍 체크 오류 {stock_code}: {e}")
            return TimingCondition(
                is_trading_allowed=False,
                reason="타이밍 체크 오류",
                risk_level="HIGH"
            )
    
    def get_timing_score(self, market_data: Dict, stock_data: Dict = None) -> float:
        """타이밍 점수 (0-100, 높을수록 좋은 타이밍)"""
        score = 100.0
        
        # 현재 시간 점수 (장 중반이 가장 좋음)
        current_time = datetime.now().time()
        if time(9, 0) <= current_time <= time(9, 30):
            score -= 20  # 장초반 감점
        elif time(14, 30) <= current_time <= time(15, 30):
            score -= 10  # 장후반 감점
        
        # 변동성 점수
        volatility = market_data.get('volatility', 0)
        if volatility > self.extreme_volatility_threshold:
            score -= 40
        elif volatility > self.high_volatility_threshold:
            score -= 20
        
        # 시장 상황 점수  
        kospi_change = market_data.get('kospi_change', 0)
        kosdaq_change = market_data.get('kosdaq_change', 0)
        avg_change = (kospi_change + kosdaq_change) / 2
        
        if avg_change < self.crash_threshold:
            score -= 30
        elif avg_change < self.bearish_threshold:
            score -= 15
        elif avg_change > 1.0:
            score += 10  # 상승장 보너스
        
        # 개별 종목 점수 (있는 경우)
        if stock_data:
            daily_change = abs(stock_data.get('daily_change_pct', 0))
            volume_ratio = stock_data.get('volume_ratio', 1.0)
            
            if daily_change > self.max_surge_change:
                score -= 25
            elif daily_change > 10:
                score -= 10
                
            if volume_ratio > self.volume_spike_threshold:
                score -= 15
            elif volume_ratio > 3:
                score -= 5
        
        return max(0, min(100, score))