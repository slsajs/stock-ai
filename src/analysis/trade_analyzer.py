from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


class TradeAnalyzer:
    def __init__(self):
        self.trades = []
        
    def add_trade_result(self, symbol: str, entry_price: float, exit_price: float, 
                        quantity: int, entry_time: datetime, exit_time: datetime, reason: str):
        """매매 결과 기록"""
        pnl = (exit_price - entry_price) * quantity
        pnl_rate = (exit_price - entry_price) / entry_price * 100
        
        trade = {
            'symbol': symbol,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'quantity': quantity,
            'pnl': pnl,
            'pnl_rate': pnl_rate,
            'entry_time': entry_time,
            'exit_time': exit_time,
            'reason': reason,
            'hold_minutes': (exit_time - entry_time).total_seconds() / 60
        }
        
        self.trades.append(trade)
        
    def analyze_performance(self, recent_days: int = 30) -> str:
        """최근 성과 분석"""
        if not self.trades:
            return "분석할 데이터가 없습니다"
            
        recent_trades = [t for t in self.trades 
                        if t['exit_time'] >= datetime.now() - timedelta(days=recent_days)]
        
        if not recent_trades:
            return f"최근 {recent_days}일간 매매 내역이 없습니다"
            
        # 통계 계산
        total_trades = len(recent_trades)
        winning_trades = [t for t in recent_trades if t['pnl'] > 0]
        losing_trades = [t for t in recent_trades if t['pnl'] < 0]
        
        win_rate = len(winning_trades) / total_trades * 100
        avg_profit = sum(t['pnl'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
        avg_loss = sum(t['pnl'] for t in losing_trades) / len(losing_trades) if losing_trades else 0
        total_pnl = sum(t['pnl'] for t in recent_trades)
        
        # 연속 손실 분석
        consecutive_losses = self.get_max_consecutive_losses(recent_trades)
        
        analysis = f"""
=== 최근 {recent_days}일 매매 분석 ===
총 매매 횟수: {total_trades}회
승률: {win_rate:.1f}%
평균 수익: {avg_profit:,.0f}원
평균 손실: {avg_loss:,.0f}원
총 손익: {total_pnl:,.0f}원
최대 연속 손실: {consecutive_losses}회

=== 손실 원인 분석 ===
{self.analyze_loss_reasons(losing_trades)}
        """
        
        return analysis
    
    def analyze_loss_reasons(self, losing_trades: List[Dict[str, Any]]) -> str:
        """손실 원인 분석"""
        if not losing_trades:
            return "손실 거래가 없습니다"
            
        reasons = {}
        for trade in losing_trades:
            reason = trade['reason']
            if reason not in reasons:
                reasons[reason] = {'count': 0, 'total_loss': 0}
            reasons[reason]['count'] += 1
            reasons[reason]['total_loss'] += abs(trade['pnl'])
            
        analysis = ""
        for reason, data in sorted(reasons.items(), key=lambda x: x[1]['total_loss'], reverse=True):
            analysis += f"- {reason}: {data['count']}회, {data['total_loss']:,.0f}원 손실\n"
            
        return analysis

    def get_max_consecutive_losses(self, trades: List[Dict[str, Any]]) -> int:
        """최대 연속 손실 횟수 계산"""
        if not trades:
            return 0
            
        max_consecutive = 0
        current_consecutive = 0
        
        # 시간 순으로 정렬
        sorted_trades = sorted(trades, key=lambda x: x['exit_time'])
        
        for trade in sorted_trades:
            if trade['pnl'] < 0:
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 0
                
        return max_consecutive

    def get_recent_trades(self, days: int = 7) -> List[Dict[str, Any]]:
        """최근 거래 내역 조회"""
        recent_trades = [t for t in self.trades 
                        if t['exit_time'] >= datetime.now() - timedelta(days=days)]
        return sorted(recent_trades, key=lambda x: x['exit_time'], reverse=True)

    def get_profit_factor(self, recent_days: int = 30) -> float:
        """수익 팩터 계산 (총 수익 / 총 손실)"""
        recent_trades = [t for t in self.trades 
                        if t['exit_time'] >= datetime.now() - timedelta(days=recent_days)]
        
        if not recent_trades:
            return 0.0
            
        total_profit = sum(t['pnl'] for t in recent_trades if t['pnl'] > 0)
        total_loss = abs(sum(t['pnl'] for t in recent_trades if t['pnl'] < 0))
        
        if total_loss == 0:
            return float('inf') if total_profit > 0 else 0.0
            
        return total_profit / total_loss

    def export_to_csv(self, filename: str) -> None:
        """거래 내역을 CSV로 내보내기"""
        import csv
        
        if not self.trades:
            print("내보낼 거래 데이터가 없습니다.")
            return
            
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['symbol', 'entry_price', 'exit_price', 'quantity', 'pnl', 'pnl_rate', 
                         'entry_time', 'exit_time', 'reason', 'hold_minutes']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for trade in self.trades:
                writer.writerow(trade)
                
        print(f"거래 내역이 {filename}에 저장되었습니다.")