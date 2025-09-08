"""
거래 빈도 제어 및 수수료 최적화 관리자
- 같은 종목 재매수 제한
- 일일 거래 횟수 제한
- 연속 손실 보호
- 최소 수익률 필터
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """거래 기록 데이터 클래스"""
    symbol: str
    action: str  # 'buy' or 'sell'
    timestamp: datetime
    price: float
    quantity: int
    profit_loss: float = 0.0
    profit_rate: float = 0.0
    reason: str = ""


@dataclass  
class StockCooldown:
    """종목별 쿨다운 정보"""
    last_sell_time: Optional[datetime] = None
    consecutive_losses: int = 0
    loss_cooldown_until: Optional[datetime] = None
    daily_trade_count: int = 0


class TradingFrequencyController:
    """거래 빈도 제어 및 수수료 최적화 관리자"""
    
    def __init__(self):
        # 설정값
        self.same_stock_cooldown_minutes = 10  # 같은 종목 재매수 대기시간 (분)
        self.max_daily_trades = 10  # 일일 최대 거래 횟수
        self.max_daily_trades_per_stock = 3  # 종목당 일일 최대 거래 횟수
        self.consecutive_loss_limit = 2  # 연속 손실 한계
        self.loss_cooldown_hours = 1  # 연속 손실 시 거래 중단 시간 (시간)
        self.min_profit_vs_fee_ratio = 2.0  # 최소 수익 vs 수수료 비율 (완화)
        
        # 수수료 설정 (한국투자증권 기준)
        self.buy_fee_rate = 0.00015  # 0.015% (최소 1원)
        self.sell_fee_rate = 0.00015 + 0.0023  # 0.015% + 0.23% (증권거래세)
        self.min_fee = 1  # 최소 수수료
        
        # 상태 관리
        self.stock_cooldowns: Dict[str, StockCooldown] = defaultdict(StockCooldown)
        self.daily_trades: List[TradeRecord] = []
        self.today = datetime.now().date()
        
        logger.info("TradingFrequencyController 초기화 완료")
        logger.info(f"설정: 재매수대기 {self.same_stock_cooldown_minutes}분, "
                   f"일일최대거래 {self.max_daily_trades}회, "
                   f"종목당최대 {self.max_daily_trades_per_stock}회")
    
    def can_buy_stock(self, symbol: str, expected_price: float, quantity: int) -> Tuple[bool, str]:
        """종목 매수 가능 여부 체크"""
        try:
            current_time = datetime.now()
            self._update_daily_reset()
            
            # 1. 일일 거래 한도 체크
            today_trades = len([t for t in self.daily_trades if t.action == 'buy'])
            if today_trades >= self.max_daily_trades:
                return False, f"일일 거래 한도 초과 ({today_trades}/{self.max_daily_trades})"
            
            # 2. 종목당 일일 거래 한도 체크
            stock_cooldown = self.stock_cooldowns[symbol]
            if stock_cooldown.daily_trade_count >= self.max_daily_trades_per_stock:
                return False, f"{symbol} 일일 거래 한도 초과 ({stock_cooldown.daily_trade_count}/{self.max_daily_trades_per_stock})"
            
            # 3. 같은 종목 재매수 쿨다운 체크
            if stock_cooldown.last_sell_time:
                cooldown_end = stock_cooldown.last_sell_time + timedelta(minutes=self.same_stock_cooldown_minutes)
                if current_time < cooldown_end:
                    remaining_minutes = (cooldown_end - current_time).total_seconds() / 60
                    return False, f"{symbol} 재매수 대기 중 ({remaining_minutes:.1f}분 남음)"
            
            # 4. 연속 손실 쿨다운 체크
            if stock_cooldown.loss_cooldown_until and current_time < stock_cooldown.loss_cooldown_until:
                remaining_minutes = (stock_cooldown.loss_cooldown_until - current_time).total_seconds() / 60
                return False, f"{symbol} 연속손실로 거래 중단 ({remaining_minutes:.1f}분 남음)"
            
            # 5. 최소 수익률 필터 체크
            expected_profit = self._calculate_minimum_profitable_exit(expected_price, quantity)
            min_exit_price = expected_profit['min_exit_price']
            profit_potential = (min_exit_price - expected_price) / expected_price * 100
            
            if profit_potential < 0.1:  # 최소 0.1% 수익 잠재력 필요 (완화)
                return False, f"수익 잠재력 부족 (최소 {min_exit_price:,.0f}원 필요, 현재 {expected_price:,.0f}원)"
            
            return True, f"매수 가능 (일일거래: {today_trades+1}/{self.max_daily_trades}, " \
                         f"종목거래: {stock_cooldown.daily_trade_count+1}/{self.max_daily_trades_per_stock})"
            
        except Exception as e:
            logger.error(f"매수 가능 여부 체크 오류: {e}")
            return False, f"체크 오류: {e}"
    
    def can_sell_stock(self, symbol: str, current_price: float, entry_price: float, quantity: int) -> Tuple[bool, str]:
        """종목 매도 가능 여부 및 수익성 체크"""
        try:
            # 손절/익절 등 긴급 매도는 항상 허용
            profit_rate = (current_price - entry_price) / entry_price * 100
            
            # 손절 상황 (항상 허용)
            if profit_rate <= -2.0:
                return True, f"손절 매도 허용 ({profit_rate:+.2f}%)"
            
            # 수익성 체크
            sell_fees = self._calculate_sell_fees(current_price, quantity)
            buy_fees = self._calculate_buy_fees(entry_price, quantity)
            total_fees = buy_fees + sell_fees
            
            gross_profit = (current_price - entry_price) * quantity
            net_profit = gross_profit - total_fees
            
            # 수수료를 고려한 순수익이 양수인지 체크
            if net_profit > 0:
                return True, f"수익성 매도 (순수익: {net_profit:,.0f}원, 수수료: {total_fees:,.0f}원)"
            else:
                # 수익성이 없더라도 일정 시간 보유 후에는 매도 허용 (포지션 관리)
                return True, f"포지션 정리 매도 (순손실: {net_profit:,.0f}원)"
            
        except Exception as e:
            logger.error(f"매도 가능 여부 체크 오류: {e}")
            return True, f"체크 오류로 매도 허용: {e}"
    
    def record_buy_trade(self, symbol: str, price: float, quantity: int, reason: str):
        """매수 거래 기록"""
        try:
            current_time = datetime.now()
            
            # 거래 기록 추가
            trade_record = TradeRecord(
                symbol=symbol,
                action='buy',
                timestamp=current_time,
                price=price,
                quantity=quantity,
                reason=reason
            )
            self.daily_trades.append(trade_record)
            
            # 종목별 통계 업데이트
            stock_cooldown = self.stock_cooldowns[symbol]
            stock_cooldown.daily_trade_count += 1
            
            # 연속 손실 리셋 (새로운 매수시)
            if stock_cooldown.loss_cooldown_until and current_time >= stock_cooldown.loss_cooldown_until:
                stock_cooldown.consecutive_losses = 0
                stock_cooldown.loss_cooldown_until = None
            
            logger.info(f"매수 거래 기록: {symbol} {quantity}주 @{price:,.0f}원 - {reason}")
            logger.info(f"일일 거래 현황: 전체 {len([t for t in self.daily_trades if t.action == 'buy'])}/{self.max_daily_trades}, "
                       f"{symbol} {stock_cooldown.daily_trade_count}/{self.max_daily_trades_per_stock}")
            
        except Exception as e:
            logger.error(f"매수 거래 기록 오류: {e}")
    
    def record_sell_trade(self, symbol: str, price: float, quantity: int, entry_price: float, reason: str):
        """매도 거래 기록 및 쿨다운 설정"""
        try:
            current_time = datetime.now()
            
            # 손익 계산
            profit_loss = (price - entry_price) * quantity
            profit_rate = (price - entry_price) / entry_price * 100
            
            # 수수료 계산
            buy_fees = self._calculate_buy_fees(entry_price, quantity)
            sell_fees = self._calculate_sell_fees(price, quantity)
            total_fees = buy_fees + sell_fees
            net_profit = profit_loss - total_fees
            
            # 거래 기록 추가
            trade_record = TradeRecord(
                symbol=symbol,
                action='sell',
                timestamp=current_time,
                price=price,
                quantity=quantity,
                profit_loss=net_profit,  # 수수료 차감 후 순손익
                profit_rate=profit_rate,
                reason=reason
            )
            self.daily_trades.append(trade_record)
            
            # 종목별 상태 업데이트
            stock_cooldown = self.stock_cooldowns[symbol]
            stock_cooldown.last_sell_time = current_time
            
            # 손실 거래 체크 및 연속 손실 관리
            if net_profit < 0:
                stock_cooldown.consecutive_losses += 1
                logger.warning(f"{symbol} 연속 손실: {stock_cooldown.consecutive_losses}회")
                
                # 연속 손실 한계 도달 시 쿨다운 적용
                if stock_cooldown.consecutive_losses >= self.consecutive_loss_limit:
                    stock_cooldown.loss_cooldown_until = current_time + timedelta(hours=self.loss_cooldown_hours)
                    logger.warning(f"{symbol} 연속 {self.consecutive_loss_limit}회 손실로 {self.loss_cooldown_hours}시간 거래 중단")
            else:
                # 수익 거래 시 연속 손실 리셋
                stock_cooldown.consecutive_losses = 0
                
            logger.info(f"매도 거래 기록: {symbol} {quantity}주 @{price:,.0f}원")
            logger.info(f"손익: {net_profit:+,.0f}원 ({profit_rate:+.2f}%), 수수료: {total_fees:,.0f}원")
            logger.info(f"재매수 가능 시간: {current_time + timedelta(minutes=self.same_stock_cooldown_minutes)}")
            
        except Exception as e:
            logger.error(f"매도 거래 기록 오류: {e}")
    
    def _calculate_buy_fees(self, price: float, quantity: int) -> float:
        """매수 수수료 계산"""
        trade_value = price * quantity
        fee = max(trade_value * self.buy_fee_rate, self.min_fee)
        return fee
    
    def _calculate_sell_fees(self, price: float, quantity: int) -> float:
        """매도 수수료 계산 (증권거래세 포함)"""
        trade_value = price * quantity
        fee = max(trade_value * self.sell_fee_rate, self.min_fee)
        return fee
    
    def _calculate_minimum_profitable_exit(self, entry_price: float, quantity: int) -> Dict:
        """최소 수익 달성을 위한 매도가 계산"""
        try:
            buy_fees = self._calculate_buy_fees(entry_price, quantity)
            
            # 최소 요구 순수익 (수수료의 4배)
            min_required_profit = buy_fees * self.min_profit_vs_fee_ratio
            
            # 매도가를 x라고 하면: (x - entry_price) * quantity - sell_fees = min_required_profit
            # x * quantity - entry_price * quantity - x * quantity * sell_fee_rate = min_required_profit
            # x * quantity * (1 - sell_fee_rate) = entry_price * quantity + min_required_profit
            # x = (entry_price * quantity + min_required_profit) / (quantity * (1 - sell_fee_rate))
            
            numerator = entry_price * quantity + min_required_profit
            denominator = quantity * (1 - self.sell_fee_rate)
            min_exit_price = numerator / denominator
            
            # 최소 수수료 고려 재계산
            sell_fees_at_min_price = self._calculate_sell_fees(min_exit_price, quantity)
            actual_profit = (min_exit_price - entry_price) * quantity - buy_fees - sell_fees_at_min_price
            
            return {
                'min_exit_price': min_exit_price,
                'required_profit': min_required_profit,
                'actual_profit': actual_profit,
                'buy_fees': buy_fees,
                'sell_fees': sell_fees_at_min_price,
                'total_fees': buy_fees + sell_fees_at_min_price
            }
            
        except Exception as e:
            logger.error(f"최소 수익 매도가 계산 오류: {e}")
            return {
                'min_exit_price': entry_price * 1.02,  # 기본 2% 상승
                'required_profit': 0,
                'actual_profit': 0,
                'buy_fees': 0,
                'sell_fees': 0,
                'total_fees': 0
            }
    
    def _update_daily_reset(self):
        """일일 리셋 체크 및 실행"""
        current_date = datetime.now().date()
        
        if current_date != self.today:
            logger.info(f"일일 통계 리셋: {self.today} -> {current_date}")
            
            # 전날 통계 요약 로그
            if self.daily_trades:
                buy_count = len([t for t in self.daily_trades if t.action == 'buy'])
                sell_count = len([t for t in self.daily_trades if t.action == 'sell'])
                total_profit = sum([t.profit_loss for t in self.daily_trades if t.action == 'sell'])
                logger.info(f"전날 거래 요약: 매수 {buy_count}회, 매도 {sell_count}회, 순손익 {total_profit:+,.0f}원")
            
            # 리셋
            self.daily_trades.clear()
            self.today = current_date
            
            # 종목별 일일 거래 카운트 리셋
            for stock_cooldown in self.stock_cooldowns.values():
                stock_cooldown.daily_trade_count = 0
    
    def get_trading_status(self) -> Dict:
        """현재 거래 제한 상태 조회"""
        try:
            self._update_daily_reset()
            current_time = datetime.now()
            
            # 전체 통계
            today_buy_count = len([t for t in self.daily_trades if t.action == 'buy'])
            today_sell_count = len([t for t in self.daily_trades if t.action == 'sell'])
            
            # 종목별 상태
            stock_status = {}
            for symbol, cooldown in self.stock_cooldowns.items():
                status = {
                    'daily_trade_count': cooldown.daily_trade_count,
                    'consecutive_losses': cooldown.consecutive_losses,
                    'can_buy': True,
                    'restrictions': []
                }
                
                # 재매수 쿨다운 체크
                if cooldown.last_sell_time:
                    cooldown_end = cooldown.last_sell_time + timedelta(minutes=self.same_stock_cooldown_minutes)
                    if current_time < cooldown_end:
                        remaining = (cooldown_end - current_time).total_seconds() / 60
                        status['can_buy'] = False
                        status['restrictions'].append(f"재매수 대기 {remaining:.1f}분")
                
                # 연속 손실 쿨다운 체크
                if cooldown.loss_cooldown_until and current_time < cooldown.loss_cooldown_until:
                    remaining = (cooldown.loss_cooldown_until - current_time).total_seconds() / 60
                    status['can_buy'] = False
                    status['restrictions'].append(f"연속손실 제한 {remaining:.1f}분")
                
                # 일일 한도 체크
                if cooldown.daily_trade_count >= self.max_daily_trades_per_stock:
                    status['can_buy'] = False
                    status['restrictions'].append("일일 종목 한도 초과")
                
                stock_status[symbol] = status
            
            return {
                'daily_stats': {
                    'buy_count': today_buy_count,
                    'sell_count': today_sell_count,
                    'max_daily_trades': self.max_daily_trades,
                    'can_trade': today_buy_count < self.max_daily_trades
                },
                'stock_status': stock_status,
                'settings': {
                    'cooldown_minutes': self.same_stock_cooldown_minutes,
                    'max_daily_trades': self.max_daily_trades,
                    'max_daily_trades_per_stock': self.max_daily_trades_per_stock,
                    'consecutive_loss_limit': self.consecutive_loss_limit,
                    'loss_cooldown_hours': self.loss_cooldown_hours
                }
            }
            
        except Exception as e:
            logger.error(f"거래 상태 조회 오류: {e}")
            return {'error': str(e)}
    
    def get_fee_analysis(self, trades: List[TradeRecord] = None) -> Dict:
        """수수료 분석 리포트"""
        try:
            if trades is None:
                trades = self.daily_trades
                
            if not trades:
                return {'message': '분석할 거래 데이터가 없습니다'}
            
            total_fees = 0
            total_gross_profit = 0
            total_net_profit = 0
            buy_trades = []
            sell_trades = []
            
            for trade in trades:
                if trade.action == 'buy':
                    buy_trades.append(trade)
                    fee = self._calculate_buy_fees(trade.price, trade.quantity)
                    total_fees += fee
                elif trade.action == 'sell':
                    sell_trades.append(trade)
                    fee = self._calculate_sell_fees(trade.price, trade.quantity)
                    total_fees += fee
                    total_net_profit += trade.profit_loss
            
            # 총 거래대금 계산
            total_trade_value = sum([(t.price * t.quantity) for t in trades])
            fee_ratio = (total_fees / total_trade_value * 100) if total_trade_value > 0 else 0
            
            return {
                'total_fees': total_fees,
                'total_trade_value': total_trade_value,
                'fee_ratio_percent': fee_ratio,
                'total_net_profit': total_net_profit,
                'profit_after_fees': total_net_profit,
                'buy_count': len(buy_trades),
                'sell_count': len(sell_trades),
                'avg_fee_per_trade': total_fees / len(trades) if trades else 0
            }
            
        except Exception as e:
            logger.error(f"수수료 분석 오류: {e}")
            return {'error': str(e)}