"""
손절/익절 관리 모듈
포지션별 손절/익절/트레일링 스탑 관리
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class PositionInfo:
    symbol: str
    entry_price: float
    quantity: int
    entry_time: datetime
    highest_price: float
    lowest_price: float
    stop_loss_price: float
    take_profit_price: float
    trailing_stop_price: Optional[float] = None
    
    def __post_init__(self):
        """초기화 후 처리"""
        if self.trailing_stop_price is None:
            self.trailing_stop_price = self.stop_loss_price
    
    @property
    def current_profit_loss_pct(self) -> float:
        """현재 손익률 (가격 업데이트 후 계산)"""
        return 0.0  # 실시간 가격은 외부에서 제공
    
    @property
    def age_minutes(self) -> float:
        """포지션 보유 시간 (분)"""
        return (datetime.now() - self.entry_time).total_seconds() / 60

class StopLossManager:
    def __init__(self, 
                 default_stop_loss_pct: float = 0.02,  # 기본 손절 2%
                 default_take_profit_pct: float = 0.03,  # 기본 익절 3%
                 trailing_stop_pct: float = 0.015,  # 트레일링 스탑 1.5%
                 max_position_time: int = 45):  # 최대 보유 시간 45분
        
        self.positions: Dict[str, PositionInfo] = {}
        self.default_stop_loss_pct = default_stop_loss_pct
        self.default_take_profit_pct = default_take_profit_pct
        self.trailing_stop_pct = trailing_stop_pct
        self.max_position_time = max_position_time
        
        # 시장 상황별 조정값
        self.market_volatility_factor = 1.0  # 변동성에 따른 조정
        self.volume_surge_factor = 1.0  # 거래량 급증시 조정
        
        logger.info(f"StopLossManager initialized - Stop Loss: {default_stop_loss_pct:.1%}, Take Profit: {default_take_profit_pct:.1%}, Trailing: {trailing_stop_pct:.1%}")
    
    def add_position(self, 
                    symbol: str, 
                    entry_price: float, 
                    quantity: int, 
                    entry_time: Optional[datetime] = None,
                    custom_stop_loss_pct: Optional[float] = None,
                    custom_take_profit_pct: Optional[float] = None) -> bool:
        """포지션 추가"""
        
        if entry_time is None:
            entry_time = datetime.now()
            
        # 커스텀 손절/익절가 또는 기본값 사용
        stop_loss_pct = custom_stop_loss_pct or self.default_stop_loss_pct
        take_profit_pct = custom_take_profit_pct or self.default_take_profit_pct
        
        # 시장 상황에 따른 조정
        adjusted_stop_loss_pct = stop_loss_pct * self.market_volatility_factor
        adjusted_take_profit_pct = take_profit_pct * self.volume_surge_factor
        
        position = PositionInfo(
            symbol=symbol,
            entry_price=entry_price,
            quantity=quantity,
            entry_time=entry_time,
            highest_price=entry_price,
            lowest_price=entry_price,
            stop_loss_price=entry_price * (1 - adjusted_stop_loss_pct),
            take_profit_price=entry_price * (1 + adjusted_take_profit_pct),
            trailing_stop_price=entry_price * (1 - self.trailing_stop_pct)
        )
        
        self.positions[symbol] = position
        
        logger.info(f"📍 Position added: {symbol} {quantity}주 @{entry_price:,.0f}원")
        logger.info(f"   손절: {position.stop_loss_price:,.0f}원 ({-adjusted_stop_loss_pct:.1%})")
        logger.info(f"   익절: {position.take_profit_price:,.0f}원 (+{adjusted_take_profit_pct:.1%})")
        logger.info(f"   트레일링: {position.trailing_stop_price:,.0f}원")
        
        return True
    
    def update_price(self, symbol: str, current_price: float) -> None:
        """가격 업데이트 및 트레일링 스탑 조정"""
        if symbol not in self.positions:
            return

        position = self.positions[symbol]

        # 최고가/최저가 업데이트
        if current_price > position.highest_price:
            position.highest_price = current_price
            # 트레일링 스탑 상향 조정
            new_trailing_stop = current_price * (1 - self.trailing_stop_pct)
            if new_trailing_stop > position.trailing_stop_price:
                old_trailing = position.trailing_stop_price
                position.trailing_stop_price = new_trailing_stop
                logger.debug(f"📈 Trailing stop updated for {symbol}: {old_trailing:,.0f} → {new_trailing_stop:,.0f}원")

        if current_price < position.lowest_price:
            position.lowest_price = current_price

    def _calculate_dynamic_holding_time(self, position: PositionInfo, current_price: float, profit_loss_pct: float) -> float:
        """추세와 수익률에 따라 동적으로 보유시간 계산"""
        base_time = self.max_position_time

        # 1. 수익률 기반 조정
        if profit_loss_pct > 2.0:  # 2% 이상 수익
            # 강한 수익 추세면 보유시간 연장 (최대 2배)
            time_multiplier = min(2.0, 1.0 + (profit_loss_pct / 10))
            adjusted_time = base_time * time_multiplier
            logger.debug(f"📈 수익률 {profit_loss_pct:.1f}% - 보유시간 연장: {base_time}분 → {adjusted_time:.0f}분")
            return adjusted_time

        elif profit_loss_pct < -1.0:  # 1% 이상 손실
            # 손실 추세면 보유시간 단축 (최소 0.6배)
            time_multiplier = max(0.6, 1.0 + (profit_loss_pct / 10))
            adjusted_time = base_time * time_multiplier
            logger.debug(f"📉 손실률 {profit_loss_pct:.1f}% - 보유시간 단축: {base_time}분 → {adjusted_time:.0f}분")
            return adjusted_time

        # 2. 추세 강도 기반 조정 (최고가 대비 현재가)
        if position.highest_price > position.entry_price:
            # 상승 후 조정 중인 경우
            pullback_pct = ((position.highest_price - current_price) / position.highest_price) * 100

            if pullback_pct > 1.5:  # 최고가 대비 1.5% 이상 하락
                # 추세 약화 - 보유시간 단축
                adjusted_time = base_time * 0.8
                logger.debug(f"⚠️ 추세 약화 (최고가 대비 -{pullback_pct:.1f}%) - 보유시간 단축: {base_time}분 → {adjusted_time:.0f}분")
                return adjusted_time
            elif pullback_pct < 0.5 and profit_loss_pct > 0.5:  # 계속 상승 중
                # 추세 강함 - 보유시간 연장
                adjusted_time = base_time * 1.3
                logger.debug(f"💪 강한 상승추세 - 보유시간 연장: {base_time}분 → {adjusted_time:.0f}분")
                return adjusted_time

        # 3. 기본값 반환
        return base_time
    
    def check_exit_signal(self, symbol: str, current_price: float) -> Optional[Tuple[str, str, Dict]]:
        """청산 신호 체크"""
        if symbol not in self.positions:
            return None
            
        position = self.positions[symbol]
        profit_loss_pct = ((current_price - position.entry_price) / position.entry_price) * 100
        
        # 추가 정보
        exit_info = {
            'entry_price': position.entry_price,
            'current_price': current_price,
            'profit_loss_pct': profit_loss_pct,
            'position_age_minutes': position.age_minutes,
            'highest_price': position.highest_price,
            'lowest_price': position.lowest_price
        }
        
        # 1. 손절 체크 (기본 손절가)
        if current_price <= position.stop_loss_price:
            reason = f"손절가 도달 ({position.stop_loss_price:,.0f}원)"
            return "손절", reason, exit_info
        
        # 2. 익절 체크 (기본 익절가)
        if current_price >= position.take_profit_price:
            reason = f"익절가 도달 ({position.take_profit_price:,.0f}원)"
            return "익절", reason, exit_info
        
        # 3. 트레일링 스탑 체크
        if current_price <= position.trailing_stop_price:
            reason = f"트레일링스탑 도달 ({position.trailing_stop_price:,.0f}원)"
            return "트레일링스탑", reason, exit_info
        
        # 4. 동적 시간 기반 청산 (추세 및 수익률에 따라 보유시간 조정)
        dynamic_max_time = self._calculate_dynamic_holding_time(position, current_price, profit_loss_pct)

        if position.age_minutes >= dynamic_max_time:
            if profit_loss_pct > 0:
                reason = f"시간만료 익절 ({int(dynamic_max_time)}분 초과)"
                return "시간익절", reason, exit_info
            else:
                reason = f"시간만료 손절 ({int(dynamic_max_time)}분 초과)"
                return "시간손절", reason, exit_info
        
        # 5. 급락/급등 보호 (5% 이상 움직임)
        if profit_loss_pct <= -5.0:
            reason = f"급락 보호 손절 ({profit_loss_pct:.1f}%)"
            return "급락손절", reason, exit_info
            
        if profit_loss_pct >= 8.0:  # 큰 수익 시 조기 익절
            reason = f"급등 보호 익절 ({profit_loss_pct:.1f}%)"
            return "급등익절", reason, exit_info
        
        # 6. RSI 기반 청산 (외부에서 RSI 값 제공받는 경우)
        # 이 부분은 기술적 분석 모듈과 연동하여 구현 가능
        
        return None
    
    def get_position_status(self, symbol: str, current_price: float) -> Optional[Dict]:
        """포지션 상태 조회"""
        if symbol not in self.positions:
            return None
            
        position = self.positions[symbol]
        profit_loss_pct = ((current_price - position.entry_price) / position.entry_price) * 100
        profit_loss_amount = (current_price - position.entry_price) * position.quantity
        
        return {
            'symbol': symbol,
            'entry_price': position.entry_price,
            'current_price': current_price,
            'quantity': position.quantity,
            'entry_time': position.entry_time,
            'age_minutes': position.age_minutes,
            'profit_loss_pct': profit_loss_pct,
            'profit_loss_amount': profit_loss_amount,
            'stop_loss_price': position.stop_loss_price,
            'take_profit_price': position.take_profit_price,
            'trailing_stop_price': position.trailing_stop_price,
            'highest_price': position.highest_price,
            'lowest_price': position.lowest_price,
            'distance_to_stop_loss': ((current_price - position.stop_loss_price) / current_price) * 100,
            'distance_to_take_profit': ((position.take_profit_price - current_price) / current_price) * 100,
            'distance_to_trailing_stop': ((current_price - position.trailing_stop_price) / current_price) * 100
        }
    
    def remove_position(self, symbol: str) -> bool:
        """포지션 제거"""
        if symbol in self.positions:
            del self.positions[symbol]
            logger.info(f"🗑️ Position removed: {symbol}")
            return True
        return False
    
    def get_all_positions_status(self, current_prices: Dict[str, float]) -> List[Dict]:
        """모든 포지션 상태 조회"""
        statuses = []
        for symbol in self.positions.keys():
            if symbol in current_prices:
                status = self.get_position_status(symbol, current_prices[symbol])
                if status:
                    statuses.append(status)
        return statuses
    
    def adjust_market_conditions(self, volatility_factor: float = 1.0, volume_surge_factor: float = 1.0):
        """시장 상황에 따른 파라미터 조정"""
        self.market_volatility_factor = max(0.5, min(2.0, volatility_factor))  # 0.5~2.0 범위
        self.volume_surge_factor = max(0.8, min(1.5, volume_surge_factor))  # 0.8~1.5 범위
        
        logger.info(f"🎛️ Market conditions adjusted - Volatility: {self.market_volatility_factor:.2f}, Volume: {self.volume_surge_factor:.2f}")
    
    def emergency_exit_all(self, current_prices: Dict[str, float], reason: str = "긴급청산") -> List[Tuple[str, str, Dict]]:
        """모든 포지션 긴급 청산"""
        exit_signals = []
        
        for symbol in list(self.positions.keys()):
            if symbol in current_prices:
                position = self.positions[symbol]
                current_price = current_prices[symbol]
                profit_loss_pct = ((current_price - position.entry_price) / position.entry_price) * 100
                
                exit_info = {
                    'entry_price': position.entry_price,
                    'current_price': current_price,
                    'profit_loss_pct': profit_loss_pct,
                    'position_age_minutes': position.age_minutes
                }
                
                exit_signals.append((symbol, reason, exit_info))
                logger.warning(f"🚨 Emergency exit signal: {symbol} - {reason}")
        
        return exit_signals
    
    def get_summary(self) -> Dict:
        """포지션 관리 요약"""
        if not self.positions:
            return {
                'total_positions': 0,
                'summary': "보유 포지션 없음"
            }
        
        total_positions = len(self.positions)
        avg_age = sum(pos.age_minutes for pos in self.positions.values()) / total_positions
        
        return {
            'total_positions': total_positions,
            'average_age_minutes': avg_age,
            'positions': list(self.positions.keys()),
            'parameters': {
                'stop_loss_pct': self.default_stop_loss_pct,
                'take_profit_pct': self.default_take_profit_pct,
                'trailing_stop_pct': self.trailing_stop_pct,
                'max_position_time': self.max_position_time,
                'market_volatility_factor': self.market_volatility_factor,
                'volume_surge_factor': self.volume_surge_factor
            }
        }

class AdvancedStopLossManager(StopLossManager):
    """고급 손절 관리자 - 추가 기능"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # 고급 기능 파라미터
        self.support_resistance_buffer = 0.005  # 지지/저항 버퍼 0.5%
        self.volume_breakout_threshold = 2.0  # 거래량 돌파 임계값
        self.momentum_threshold = 0.02  # 모멘텀 임계값 2%
        
    def add_support_resistance_levels(self, symbol: str, support_level: float, resistance_level: float):
        """지지/저항 레벨 기반 손절/익절 조정"""
        if symbol not in self.positions:
            return False
            
        position = self.positions[symbol]
        
        # 지지선 기반 손절가 조정
        support_stop = support_level * (1 - self.support_resistance_buffer)
        if support_stop > position.stop_loss_price:
            position.stop_loss_price = support_stop
            logger.info(f"📊 Stop loss adjusted by support level: {symbol} → {support_stop:,.0f}원")
        
        # 저항선 기반 익절가 조정
        resistance_profit = resistance_level * (1 - self.support_resistance_buffer)
        if resistance_profit < position.take_profit_price:
            position.take_profit_price = resistance_profit
            logger.info(f"📊 Take profit adjusted by resistance level: {symbol} → {resistance_profit:,.0f}원")
        
        return True
    
    def check_volume_breakout_exit(self, symbol: str, current_price: float, current_volume: int, avg_volume: float) -> Optional[Tuple[str, str, Dict]]:
        """거래량 돌파 기반 청산 신호"""
        if symbol not in self.positions:
            return None
            
        position = self.positions[symbol]
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        if volume_ratio >= self.volume_breakout_threshold:
            profit_loss_pct = ((current_price - position.entry_price) / position.entry_price) * 100
            
            exit_info = {
                'entry_price': position.entry_price,
                'current_price': current_price,
                'profit_loss_pct': profit_loss_pct,
                'volume_ratio': volume_ratio,
                'avg_volume': avg_volume,
                'current_volume': current_volume
            }
            
            if profit_loss_pct > 1.0:  # 수익 중 거래량 급증시 익절
                reason = f"거래량 돌파 익절 (volume ratio: {volume_ratio:.1f}x)"
                return "거래량익절", reason, exit_info
            elif profit_loss_pct < -1.0:  # 손실 중 거래량 급증시 손절
                reason = f"거래량 돌파 손절 (volume ratio: {volume_ratio:.1f}x)"
                return "거래량손절", reason, exit_info
        
        return None