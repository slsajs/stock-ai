import logging
from datetime import datetime, timedelta
from typing import Optional
from ..analysis.trade_analyzer import TradeAnalyzer


def daily_performance_report(trade_analyzer: Optional[TradeAnalyzer] = None) -> str:
    """일일 성과 리포트 생성"""
    logger = logging.getLogger(__name__)
    
    try:
        if trade_analyzer is None:
            trade_analyzer = TradeAnalyzer()
        
        # 일일 성과 분석
        daily_report = trade_analyzer.analyze_performance(recent_days=1)
        
        # 주간 성과 분석 추가
        weekly_report = trade_analyzer.analyze_performance(recent_days=7)
        
        # 수익 팩터 계산
        daily_profit_factor = trade_analyzer.get_profit_factor(recent_days=1)
        weekly_profit_factor = trade_analyzer.get_profit_factor(recent_days=7)
        
        # 최근 거래 내역
        recent_trades = trade_analyzer.get_recent_trades(days=1)
        
        report = f"""
📊 === 일일 매매 성과 리포트 ({datetime.now().strftime('%Y-%m-%d')}) ===

{daily_report}

📈 === 주간 성과 요약 ===
{weekly_report}

💰 === 수익 팩터 ===
일일 수익 팩터: {daily_profit_factor:.2f}
주간 수익 팩터: {weekly_profit_factor:.2f}

📋 === 오늘의 거래 내역 ===
"""
        
        if recent_trades:
            for i, trade in enumerate(recent_trades[:5], 1):  # 최대 5개만 표시
                pnl_symbol = "📈" if trade['pnl'] > 0 else "📉"
                report += f"""
{i}. {trade['symbol']} {pnl_symbol}
   - 진입: {trade['entry_price']:,}원 → 청산: {trade['exit_price']:,}원
   - 손익: {trade['pnl']:,}원 ({trade['pnl_rate']:+.2f}%)
   - 보유시간: {trade['hold_minutes']:.0f}분
   - 사유: {trade['reason']}
"""
        else:
            report += "\n오늘 거래 내역이 없습니다.\n"
            
        report += f"\n🤖 리포트 생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        logger.info("일일 성과 리포트 생성 완료")
        return report
        
    except Exception as e:
        logger.error(f"일일 성과 리포트 생성 오류: {e}")
        return f"리포트 생성 중 오류가 발생했습니다: {e}"


async def send_daily_report_to_telegram(trade_analyzer: Optional[TradeAnalyzer] = None, config=None):
    """일일 리포트를 텔레그램으로 전송 (주석 처리됨)"""
    logger = logging.getLogger(__name__)
    
    try:
        # 리포트 생성
        report = daily_performance_report(trade_analyzer)
        
        # 텔레그램 전송 (주석 처리)
        # if config:
        #     from . import send_telegram_message
        #     await send_telegram_message(report, config)
        #     logger.info("일일 리포트 텔레그램 전송 완료")
        # else:
        #     logger.warning("텔레그램 설정이 없어서 리포트를 전송하지 않았습니다")
        
        # 콘솔에 출력
        print("\n" + "="*60)
        print(report)
        print("="*60 + "\n")
        
        logger.info("일일 리포트 생성 및 출력 완료")
        
    except Exception as e:
        logger.error(f"일일 리포트 전송 오류: {e}")


def save_daily_report_to_file(trade_analyzer: Optional[TradeAnalyzer] = None, 
                              filename: Optional[str] = None) -> str:
    """일일 리포트를 파일로 저장"""
    logger = logging.getLogger(__name__)
    
    try:
        if filename is None:
            filename = f"daily_report_{datetime.now().strftime('%Y%m%d')}.txt"
        
        report = daily_performance_report(trade_analyzer)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"일일 리포트가 {filename}에 저장되었습니다")
        return filename
        
    except Exception as e:
        logger.error(f"일일 리포트 파일 저장 오류: {e}")
        return ""


def get_performance_summary(trade_analyzer: Optional[TradeAnalyzer] = None, days: int = 30) -> dict:
    """성과 요약 정보를 딕셔너리로 반환"""
    logger = logging.getLogger(__name__)
    
    try:
        if trade_analyzer is None:
            trade_analyzer = TradeAnalyzer()
        
        recent_trades = [t for t in trade_analyzer.trades 
                        if t['exit_time'] >= datetime.now() - timedelta(days=days)]
        
        if not recent_trades:
            return {
                'total_trades': 0,
                'win_rate': 0.0,
                'total_pnl': 0,
                'avg_profit': 0,
                'avg_loss': 0,
                'profit_factor': 0.0,
                'max_consecutive_losses': 0
            }
        
        winning_trades = [t for t in recent_trades if t['pnl'] > 0]
        losing_trades = [t for t in recent_trades if t['pnl'] < 0]
        
        total_trades = len(recent_trades)
        win_rate = len(winning_trades) / total_trades * 100
        total_pnl = sum(t['pnl'] for t in recent_trades)
        avg_profit = sum(t['pnl'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
        avg_loss = sum(t['pnl'] for t in losing_trades) / len(losing_trades) if losing_trades else 0
        profit_factor = trade_analyzer.get_profit_factor(days)
        max_consecutive_losses = trade_analyzer.get_max_consecutive_losses(recent_trades)
        
        return {
            'total_trades': total_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_profit': avg_profit,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'max_consecutive_losses': max_consecutive_losses
        }
        
    except Exception as e:
        logger.error(f"성과 요약 생성 오류: {e}")
        return {}