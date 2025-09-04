"""
거래 빈도 제어 및 수수료 최적화 대시보드
실시간 모니터링 및 통계 리포트 제공
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
from .trading_frequency_controller import TradingFrequencyController

logger = logging.getLogger(__name__)


class FrequencyDashboard:
    """거래 빈도 제어 대시보드"""
    
    def __init__(self, frequency_controller: TradingFrequencyController):
        self.freq_controller = frequency_controller
        
    def generate_daily_report(self) -> str:
        """일일 거래 빈도 및 수수료 리포트 생성"""
        try:
            # 거래 상태 조회
            trading_status = self.freq_controller.get_trading_status()
            fee_analysis = self.freq_controller.get_fee_analysis()
            
            report = "📊 일일 거래 빈도 제어 리포트\n"
            report += "=" * 40 + "\n\n"
            
            # 전체 거래 통계
            daily_stats = trading_status.get('daily_stats', {})
            report += f"🔢 전체 거래 현황:\n"
            report += f"  • 매수 거래: {daily_stats.get('buy_count', 0)}회 / {daily_stats.get('max_daily_trades', 0)}회\n"
            report += f"  • 매도 거래: {daily_stats.get('sell_count', 0)}회\n"
            report += f"  • 거래 가능: {'✅ 가능' if daily_stats.get('can_trade', False) else '🚫 불가능'}\n\n"
            
            # 종목별 상태
            stock_status = trading_status.get('stock_status', {})
            if stock_status:
                report += f"📈 종목별 거래 현황:\n"
                for symbol, status in stock_status.items():
                    restrictions = status.get('restrictions', [])
                    restriction_text = ", ".join(restrictions) if restrictions else "제한 없음"
                    report += f"  • {symbol}: {status.get('daily_trade_count', 0)}회, "
                    report += f"연속손실: {status.get('consecutive_losses', 0)}회, "
                    report += f"상태: {'✅' if status.get('can_buy', True) else '🚫'} ({restriction_text})\n"
                report += "\n"
            
            # 수수료 분석
            if 'error' not in fee_analysis:
                report += f"💰 수수료 분석:\n"
                report += f"  • 총 수수료: {fee_analysis.get('total_fees', 0):,.0f}원\n"
                report += f"  • 총 거래대금: {fee_analysis.get('total_trade_value', 0):,.0f}원\n"
                report += f"  • 수수료 비율: {fee_analysis.get('fee_ratio_percent', 0):.3f}%\n"
                report += f"  • 순 손익: {fee_analysis.get('total_net_profit', 0):+,.0f}원\n"
                report += f"  • 거래당 평균 수수료: {fee_analysis.get('avg_fee_per_trade', 0):,.0f}원\n\n"
            
            # 설정 정보
            settings = trading_status.get('settings', {})
            report += f"⚙️ 거래 제어 설정:\n"
            report += f"  • 재매수 대기시간: {settings.get('cooldown_minutes', 0)}분\n"
            report += f"  • 일일 최대 거래: {settings.get('max_daily_trades', 0)}회\n"
            report += f"  • 종목당 최대 거래: {settings.get('max_daily_trades_per_stock', 0)}회\n"
            report += f"  • 연속 손실 한계: {settings.get('consecutive_loss_limit', 0)}회\n"
            report += f"  • 손실 후 대기시간: {settings.get('loss_cooldown_hours', 0)}시간\n"
            
            return report
            
        except Exception as e:
            logger.error(f"일일 리포트 생성 오류: {e}")
            return f"리포트 생성 실패: {e}"
    
    def get_real_time_status(self) -> Dict[str, Any]:
        """실시간 상태 조회"""
        try:
            current_time = datetime.now()
            trading_status = self.freq_controller.get_trading_status()
            
            # 간단한 요약 상태
            daily_stats = trading_status.get('daily_stats', {})
            status = {
                'timestamp': current_time.isoformat(),
                'can_trade_more': daily_stats.get('can_trade', False),
                'remaining_trades': max(0, daily_stats.get('max_daily_trades', 0) - daily_stats.get('buy_count', 0)),
                'total_buy_trades': daily_stats.get('buy_count', 0),
                'total_sell_trades': daily_stats.get('sell_count', 0),
                'restricted_stocks': []
            }
            
            # 제한된 종목 목록
            stock_status = trading_status.get('stock_status', {})
            for symbol, stock_info in stock_status.items():
                if not stock_info.get('can_buy', True):
                    restrictions = stock_info.get('restrictions', [])
                    status['restricted_stocks'].append({
                        'symbol': symbol,
                        'restrictions': restrictions
                    })
            
            return status
            
        except Exception as e:
            logger.error(f"실시간 상태 조회 오류: {e}")
            return {'error': str(e)}
    
    def get_fee_efficiency_metrics(self) -> Dict[str, Any]:
        """수수료 효율성 메트릭"""
        try:
            fee_analysis = self.freq_controller.get_fee_analysis()
            
            if 'error' in fee_analysis:
                return fee_analysis
            
            # 수수료 효율성 계산
            total_fees = fee_analysis.get('total_fees', 0)
            total_net_profit = fee_analysis.get('total_net_profit', 0)
            total_trades = fee_analysis.get('buy_count', 0) + fee_analysis.get('sell_count', 0)
            
            metrics = {
                'fee_to_profit_ratio': (total_fees / abs(total_net_profit) * 100) if total_net_profit != 0 else 0,
                'profit_after_fees': total_net_profit,
                'avg_fee_per_trade': fee_analysis.get('avg_fee_per_trade', 0),
                'fee_efficiency_score': 0,
                'recommendation': ""
            }
            
            # 효율성 점수 계산 (0-100)
            if total_net_profit > 0:
                fee_ratio = total_fees / total_net_profit
                if fee_ratio < 0.1:  # 수수료가 수익의 10% 미만
                    metrics['fee_efficiency_score'] = 90 + min(10, (0.1 - fee_ratio) * 100)
                    metrics['recommendation'] = "매우 효율적인 거래 패턴"
                elif fee_ratio < 0.2:  # 10-20%
                    metrics['fee_efficiency_score'] = 70 + (0.2 - fee_ratio) * 200
                    metrics['recommendation'] = "효율적인 거래 패턴"
                elif fee_ratio < 0.5:  # 20-50%
                    metrics['fee_efficiency_score'] = 30 + (0.5 - fee_ratio) * 133
                    metrics['recommendation'] = "보통 수준, 거래 횟수 조정 필요"
                else:  # 50% 이상
                    metrics['fee_efficiency_score'] = max(0, 30 - (fee_ratio - 0.5) * 60)
                    metrics['recommendation'] = "비효율적, 거래 전략 재검토 필요"
            else:
                metrics['fee_efficiency_score'] = 0
                metrics['recommendation'] = "손실 상황, 거래 패턴 전면 재검토"
            
            return metrics
            
        except Exception as e:
            logger.error(f"수수료 효율성 메트릭 계산 오류: {e}")
            return {'error': str(e)}
    
    def generate_weekly_summary(self, days: int = 7) -> str:
        """주간 요약 리포트 (추후 확장 가능)"""
        try:
            current_status = self.get_real_time_status()
            efficiency_metrics = self.get_fee_efficiency_metrics()
            
            report = f"📅 {days}일 거래 요약 리포트\n"
            report += "=" * 40 + "\n\n"
            
            # 현재는 일일 데이터만 제공하지만 추후 히스토리 DB 연결 시 확장
            report += "⚠️ 현재는 일일 데이터만 제공됩니다.\n"
            report += "히스토리 데이터베이스 구축 후 주간/월간 분석이 가능합니다.\n\n"
            
            # 현재 효율성 메트릭
            if 'error' not in efficiency_metrics:
                score = efficiency_metrics.get('fee_efficiency_score', 0)
                recommendation = efficiency_metrics.get('recommendation', '')
                
                report += f"💯 현재 수수료 효율성 점수: {score:.1f}/100\n"
                report += f"📝 권장사항: {recommendation}\n\n"
            
            return report
            
        except Exception as e:
            logger.error(f"주간 요약 생성 오류: {e}")
            return f"주간 요약 생성 실패: {e}"
    
    async def start_monitoring(self, interval_seconds: int = 300):
        """주기적 모니터링 시작 (5분 간격)"""
        try:
            logger.info(f"FrequencyDashboard 모니터링 시작 (간격: {interval_seconds}초)")
            
            while True:
                try:
                    # 현재 상태 체크
                    status = self.get_real_time_status()
                    
                    if 'error' not in status:
                        # 거래 제한 상황 알림
                        if not status.get('can_trade_more', True):
                            logger.warning("🚫 일일 거래 한도 도달!")
                        
                        restricted = status.get('restricted_stocks', [])
                        if restricted:
                            logger.info(f"⏸️ 거래 제한 종목: {len(restricted)}개")
                            for stock in restricted:
                                logger.debug(f"  - {stock['symbol']}: {stock['restrictions']}")
                        
                        # 수수료 효율성 체크
                        efficiency = self.get_fee_efficiency_metrics()
                        if 'error' not in efficiency:
                            score = efficiency.get('fee_efficiency_score', 0)
                            if score < 50:
                                logger.warning(f"⚠️ 수수료 효율성 낮음: {score:.1f}/100")
                    
                    await asyncio.sleep(interval_seconds)
                    
                except Exception as e:
                    logger.error(f"모니터링 루프 오류: {e}")
                    await asyncio.sleep(60)  # 오류 시 1분 대기
                    
        except KeyboardInterrupt:
            logger.info("FrequencyDashboard 모니터링 중단")
        except Exception as e:
            logger.error(f"모니터링 시스템 오류: {e}")
    
    def export_trading_log(self, format_type: str = "csv") -> str:
        """거래 로그 내보내기"""
        try:
            # 현재 거래 기록 조회
            trades = self.freq_controller.daily_trades
            
            if not trades:
                return "내보낼 거래 기록이 없습니다."
            
            if format_type.lower() == "csv":
                import csv
                import io
                
                output = io.StringIO()
                fieldnames = ['timestamp', 'symbol', 'action', 'price', 'quantity', 'profit_loss', 'profit_rate', 'reason']
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                
                writer.writeheader()
                for trade in trades:
                    writer.writerow({
                        'timestamp': trade.timestamp.isoformat(),
                        'symbol': trade.symbol,
                        'action': trade.action,
                        'price': trade.price,
                        'quantity': trade.quantity,
                        'profit_loss': trade.profit_loss,
                        'profit_rate': trade.profit_rate,
                        'reason': trade.reason
                    })
                
                return output.getvalue()
            
            elif format_type.lower() == "json":
                import json
                
                trade_data = []
                for trade in trades:
                    trade_data.append({
                        'timestamp': trade.timestamp.isoformat(),
                        'symbol': trade.symbol,
                        'action': trade.action,
                        'price': trade.price,
                        'quantity': trade.quantity,
                        'profit_loss': trade.profit_loss,
                        'profit_rate': trade.profit_rate,
                        'reason': trade.reason
                    })
                
                return json.dumps(trade_data, indent=2, ensure_ascii=False)
            
            else:
                return f"지원하지 않는 포맷: {format_type}"
                
        except Exception as e:
            logger.error(f"거래 로그 내보내기 오류: {e}")
            return f"내보내기 실패: {e}"