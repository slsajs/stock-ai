"""
리스크 관리 모듈
단타 매매 시스템의 리스크 관리와 손실 분석 기능을 제공
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import json
import os
from dataclasses import dataclass, asdict

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)

@dataclass
class TradeRecord:
    timestamp: datetime
    symbol: str
    action: str  # 'buy' or 'sell'
    quantity: int
    price: float
    amount: float
    reason: str
    profit_loss: float = 0.0
    profit_loss_pct: float = 0.0

class RiskManager:
    def __init__(self, initial_balance=1000000):
        # 리스크 파라미터
        self.max_loss_per_trade = 0.02      # 거래당 최대 2% 손실
        self.max_daily_loss = 0.05          # 일일 최대 5% 손실
        self.trailing_stop_rate = 0.015     # 트레일링 스탑 1.5%
        self.max_consecutive_losses = 3      # 연속 손실 3회 제한
        
        # 계좌 관리
        self.initial_balance = initial_balance
        self.current_balance = initial_balance
        self.daily_pnl = 0
        self.consecutive_losses = 0
        self.today_trade_count = 0
        self.last_reset_date = datetime.now().date()
        
        # 매매 내역 저장
        self.trade_history: List[TradeRecord] = []
        self.daily_stats = {}
        
        # 성과 추적 (Kelly Criterion 계산용)
        self.recent_trades = []  # 최근 20거래 저장
        self.win_trades = []
        self.loss_trades = []
        
    def reset_daily_data(self):
        """일일 데이터 초기화"""
        current_date = datetime.now().date()
        if current_date != self.last_reset_date:
            logger.info(f"Daily data reset - Date: {current_date}")
            self.daily_pnl = 0
            self.today_trade_count = 0
            self.last_reset_date = current_date
            
            # 연속 손실 계산 (어제까지의 거래만 고려)
            self._update_consecutive_losses()
    
    def _update_consecutive_losses(self):
        """연속 손실 횟수 업데이트"""
        if not self.trade_history:
            self.consecutive_losses = 0
            return
            
        # 최근 거래부터 역순으로 연속 손실 계산
        consecutive = 0
        for trade in reversed(self.trade_history):
            if trade.action == 'sell' and trade.profit_loss < 0:
                consecutive += 1
            elif trade.action == 'sell' and trade.profit_loss > 0:
                break
            
        self.consecutive_losses = consecutive
        logger.info(f"Consecutive losses updated: {self.consecutive_losses}")
    
    def can_trade(self) -> Tuple[bool, str]:
        """거래 가능 여부 판단"""
        self.reset_daily_data()
        
        # 일일 손실 한도 체크
        daily_loss_limit = -self.max_daily_loss * self.initial_balance
        if self.daily_pnl <= daily_loss_limit:
            return False, f"일일 손실 한도 초과 ({self.daily_pnl:,.0f}원 <= {daily_loss_limit:,.0f}원)"
            
        # 연속 손실 체크
        if self.consecutive_losses >= self.max_consecutive_losses:
            return False, f"연속 손실 한도 초과 ({self.consecutive_losses}회 >= {self.max_consecutive_losses}회)"
            
        # 잔고 최소 한도 체크
        min_balance = self.initial_balance * 0.5  # 초기 잔고의 50% 이하로 떨어지면 거래 중단
        if self.current_balance <= min_balance:
            return False, f"잔고 부족 ({self.current_balance:,.0f}원 <= {min_balance:,.0f}원)"
            
        return True, "거래 가능"
    
    def calculate_position_size(self, current_price: float, symbol: str = None) -> int:
        """Kelly Criterion 기반 포지션 사이징"""
        # 최근 성과 분석
        win_rate, avg_profit_rate, avg_loss_rate = self._calculate_trading_stats()
        
        if win_rate == 0 or avg_profit_rate == 0:
            # 초기값: 잔고의 5% (조금 더 적극적인 시작)
            safe_amount = self.current_balance * 0.05
            quantity = int(safe_amount / current_price)
            logger.info(f"Initial position sizing: {quantity}주 (safe 5% = {safe_amount:,.0f}원)")
            return max(1, quantity)
            
        # Kelly 공식: f = (bp - q) / b
        # b = 평균수익률, p = 승률, q = 패률
        if avg_profit_rate > 0:
            kelly_ratio = (avg_profit_rate * win_rate - avg_loss_rate * (1 - win_rate)) / avg_profit_rate
        else:
            kelly_ratio = 0.02
            
        # 안전하게 Kelly의 50%만 사용 (최대 15%, 최소 2%)
        safe_ratio = max(0.02, min(0.15, kelly_ratio * 0.5))
        
        # 거래당 최대 손실 한도도 고려
        max_loss_amount = self.current_balance * self.max_loss_per_trade
        max_quantity_by_loss = int(max_loss_amount / (current_price * 0.015))  # 1.5% 손절 가정 (더 타이트한 손절)
        
        # 주식 수량 계산
        kelly_amount = self.current_balance * safe_ratio
        kelly_quantity = int(kelly_amount / current_price)
        
        # 두 조건 중 더 보수적인 것 선택
        final_quantity = min(kelly_quantity, max_quantity_by_loss)
        final_quantity = max(1, final_quantity)  # 최소 1주
        
        logger.info(f"Position sizing - Win Rate: {win_rate:.1%}, Kelly: {kelly_ratio:.3f}, Safe Ratio: {safe_ratio:.3f}")
        logger.info(f"Kelly quantity: {kelly_quantity}주, Max loss quantity: {max_quantity_by_loss}주, Final: {final_quantity}주")
        
        return final_quantity
    
    def _calculate_trading_stats(self) -> Tuple[float, float, float]:
        """최근 거래 통계 계산"""
        if not self.trade_history:
            return 0.0, 0.0, 0.0
            
        # 최근 20거래 또는 전체 거래 중 적은 것
        recent_sells = [t for t in self.trade_history if t.action == 'sell'][-20:]
        
        if not recent_sells:
            return 0.0, 0.0, 0.0
            
        wins = [t for t in recent_sells if t.profit_loss > 0]
        losses = [t for t in recent_sells if t.profit_loss < 0]
        
        win_rate = len(wins) / len(recent_sells) if recent_sells else 0
        avg_profit_rate = sum(t.profit_loss_pct for t in wins) / len(wins) / 100 if wins else 0
        avg_loss_rate = abs(sum(t.profit_loss_pct for t in losses) / len(losses) / 100) if losses else 0.02
        
        return win_rate, avg_profit_rate, avg_loss_rate
    
    def record_trade(self, symbol: str, action: str, quantity: int, price: float, reason: str, profit_loss: float = 0.0, profit_loss_pct: float = 0.0):
        """거래 기록"""
        trade_record = TradeRecord(
            timestamp=datetime.now(),
            symbol=symbol,
            action=action,
            quantity=quantity,
            price=price,
            amount=quantity * price,
            reason=reason,
            profit_loss=profit_loss,
            profit_loss_pct=profit_loss_pct
        )
        
        self.trade_history.append(trade_record)
        
        # 일일 손익 업데이트
        if action == 'sell':
            self.daily_pnl += profit_loss
            self.current_balance += profit_loss
            self.today_trade_count += 1
            
            # 최근 거래 추적 (성과 분석용)
            self.recent_trades.append(trade_record)
            if len(self.recent_trades) > 20:
                self.recent_trades.pop(0)
                
            # 승/패 분류
            if profit_loss > 0:
                self.win_trades.append(trade_record)
                if len(self.win_trades) > 10:
                    self.win_trades.pop(0)
            else:
                self.loss_trades.append(trade_record)
                if len(self.loss_trades) > 10:
                    self.loss_trades.pop(0)
        
        logger.info(f"Trade recorded: {action.upper()} {symbol} {quantity}주 @{price:,.0f}원 - {reason}")
        if action == 'sell':
            logger.info(f"P&L: {profit_loss:+,.0f}원 ({profit_loss_pct:+.2f}%), Daily P&L: {self.daily_pnl:+,.0f}원")
    
    def get_daily_summary(self) -> Dict:
        """일일 거래 요약"""
        self.reset_daily_data()
        
        today_trades = [t for t in self.trade_history if t.timestamp.date() == datetime.now().date()]
        today_sells = [t for t in today_trades if t.action == 'sell']
        
        total_pnl = sum(t.profit_loss for t in today_sells)
        total_trades = len(today_sells)
        win_trades = len([t for t in today_sells if t.profit_loss > 0])
        
        return {
            'date': datetime.now().date().isoformat(),
            'total_trades': total_trades,
            'win_trades': win_trades,
            'win_rate': win_trades / total_trades if total_trades > 0 else 0,
            'total_pnl': total_pnl,
            'daily_pnl': self.daily_pnl,
            'current_balance': self.current_balance,
            'consecutive_losses': self.consecutive_losses,
            'can_trade': self.can_trade()[0]
        }
    
    def get_performance_analysis(self) -> Dict:
        """성과 분석"""
        if not self.trade_history:
            return {}
            
        sells = [t for t in self.trade_history if t.action == 'sell']
        if not sells:
            return {}
            
        total_trades = len(sells)
        wins = [t for t in sells if t.profit_loss > 0]
        losses = [t for t in sells if t.profit_loss < 0]
        
        total_pnl = sum(t.profit_loss for t in sells)
        win_rate = len(wins) / total_trades
        avg_win = sum(t.profit_loss for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t.profit_loss for t in losses) / len(losses) if losses else 0
        
        # 최대 연속 손실/이익
        max_consecutive_loss = self._calculate_max_consecutive_losses()
        max_consecutive_win = self._calculate_max_consecutive_wins()
        
        # 최대 손실 (MDD 간이 계산)
        running_balance = self.initial_balance
        peak_balance = self.initial_balance
        max_drawdown = 0
        
        for trade in sells:
            running_balance += trade.profit_loss
            if running_balance > peak_balance:
                peak_balance = running_balance
            drawdown = (peak_balance - running_balance) / peak_balance
            max_drawdown = max(max_drawdown, drawdown)
        
        return {
            'total_trades': total_trades,
            'win_trades': len(wins),
            'loss_trades': len(losses),
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': abs(avg_win / avg_loss) if avg_loss != 0 else float('inf'),
            'max_consecutive_loss': max_consecutive_loss,
            'max_consecutive_win': max_consecutive_win,
            'max_drawdown': max_drawdown,
            'current_balance': self.current_balance,
            'total_return': (self.current_balance - self.initial_balance) / self.initial_balance
        }
    
    def _calculate_max_consecutive_losses(self) -> int:
        """최대 연속 손실 횟수"""
        sells = [t for t in self.trade_history if t.action == 'sell']
        max_consecutive = 0
        current_consecutive = 0
        
        for trade in sells:
            if trade.profit_loss < 0:
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 0
                
        return max_consecutive
    
    def _calculate_max_consecutive_wins(self) -> int:
        """최대 연속 이익 횟수"""
        sells = [t for t in self.trade_history if t.action == 'sell']
        max_consecutive = 0
        current_consecutive = 0
        
        for trade in sells:
            if trade.profit_loss > 0:
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 0
                
        return max_consecutive
    
    def save_to_file(self, filename: str = None):
        """거래 기록을 파일에 저장"""
        if filename is None:
            filename = f"risk_manager_data_{datetime.now().strftime('%Y%m%d')}.json"
        
        data = {
            'initial_balance': self.initial_balance,
            'current_balance': self.current_balance,
            'daily_pnl': self.daily_pnl,
            'consecutive_losses': self.consecutive_losses,
            'today_trade_count': self.today_trade_count,
            'last_reset_date': self.last_reset_date.isoformat(),
            'trade_history': [asdict(trade) for trade in self.trade_history],
            'performance': self.get_performance_analysis(),
            'daily_summary': self.get_daily_summary()
        }
        
        # datetime 객체를 문자열로 변환
        for trade in data['trade_history']:
            if isinstance(trade['timestamp'], datetime):
                trade['timestamp'] = trade['timestamp'].isoformat()
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"Risk manager data saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save risk manager data: {e}")
    
    def load_from_file(self, filename: str):
        """파일에서 거래 기록 로드"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.initial_balance = data.get('initial_balance', self.initial_balance)
            self.current_balance = data.get('current_balance', self.current_balance)
            self.daily_pnl = data.get('daily_pnl', 0)
            self.consecutive_losses = data.get('consecutive_losses', 0)
            self.today_trade_count = data.get('today_trade_count', 0)
            
            # 거래 기록 복원
            self.trade_history = []
            for trade_data in data.get('trade_history', []):
                trade = TradeRecord(
                    timestamp=datetime.fromisoformat(trade_data['timestamp']),
                    symbol=trade_data['symbol'],
                    action=trade_data['action'],
                    quantity=trade_data['quantity'],
                    price=trade_data['price'],
                    amount=trade_data['amount'],
                    reason=trade_data['reason'],
                    profit_loss=trade_data.get('profit_loss', 0.0),
                    profit_loss_pct=trade_data.get('profit_loss_pct', 0.0)
                )
                self.trade_history.append(trade)
            
            logger.info(f"Risk manager data loaded from {filename}")
            logger.info(f"Loaded {len(self.trade_history)} trade records")
            
        except FileNotFoundError:
            logger.info(f"Risk manager data file {filename} not found, starting fresh")
        except Exception as e:
            logger.error(f"Failed to load risk manager data: {e}")