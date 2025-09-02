import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import logging

from .risk_manager import RiskManager
from .stop_loss_manager import StopLossManager
from ..analysis.market_analyzer import MarketAnalyzer
from ..analysis.enhanced_signal_analyzer import EnhancedSignalAnalyzer
from ..analysis.trade_analyzer import TradeAnalyzer


class EnhancedTradingSystem:
    """강화된 리스크 관리 매매 시스템"""
    
    def __init__(self, api_client, config):
        self.api_client = api_client
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 각종 분석기 및 관리자 초기화
        self.risk_manager = RiskManager()
        self.stop_manager = StopLossManager()
        self.market_analyzer = MarketAnalyzer()
        self.signal_analyzer = EnhancedSignalAnalyzer()
        self.trade_analyzer = TradeAnalyzer()
        
        self.is_running = False
        
    async def start_enhanced_trading(self):
        """강화된 매매 시스템 시작"""
        self.is_running = True
        self.logger.info("Enhanced Trading System 시작")
        
        while self.is_running:
            try:
                await self._trading_loop()
                await asyncio.sleep(1)  # 1초 대기
                
            except KeyboardInterrupt:
                self.logger.info("Enhanced Trading System 중단")
                break
            except Exception as e:
                self.logger.error(f"Enhanced Trading System 오류: {e}")
                await asyncio.sleep(5)  # 오류 시 5초 대기 후 재시작
                
    async def _trading_loop(self):
        """메인 트레이딩 루프"""
        # 1. 리스크 체크
        can_trade, reason = self.risk_manager.can_trade()
        if not can_trade:
            self.logger.warning(f"매매 중단: {reason}")
            return
            
        # 2. 시장 상황 분석
        market_condition = await self._get_market_condition()
        
        # 3. 실시간 데이터 받기
        tick_data = await self._get_realtime_data()
        if not tick_data:
            return
            
        # 4. 포지션 관리 (기존 보유 종목)
        await self._manage_existing_positions(tick_data)
        
        # 5. 신규 매수 신호 체크
        if not self.stop_manager.positions:  # 무포지션일 때만
            await self._check_new_buy_signals(tick_data, market_condition)
            
    async def _get_market_condition(self) -> str:
        """시장 상황 분석"""
        try:
            # KOSPI 지수 조회
            kospi_data = await self.api_client.get_index("0001")  # KOSPI
            if kospi_data and kospi_data.get('rt_cd') == '0':
                return self.market_analyzer.analyze_market_condition(kospi_data)
            return "NORMAL"
        except Exception as e:
            self.logger.error(f"시장 상황 분석 오류: {e}")
            return "NORMAL"
            
    async def _get_realtime_data(self) -> Optional[Dict[str, Any]]:
        """실시간 데이터 조회"""
        try:
            # 주요 관심 종목들의 현재가 조회
            target_stocks = self.config.target_stocks
            if not target_stocks:
                return None
                
            # 첫 번째 종목 데이터 조회 (실제로는 모든 종목 조회 필요)
            symbol = target_stocks[0]
            result = await self.api_client.get_current_price(symbol)
            
            if result and result.get('rt_cd') == '0':
                output = result['output']
                return {
                    'symbol': symbol,
                    'price': int(output['stck_prpr']),
                    'volume': int(output['acml_vol']),
                    'change_rate': float(output['prdy_ctrt']),
                    'timestamp': datetime.now()
                }
            return None
        except Exception as e:
            self.logger.error(f"실시간 데이터 조회 오류: {e}")
            return None
            
    async def _manage_existing_positions(self, tick_data: Dict[str, Any]):
        """기존 포지션 관리"""
        positions_to_exit = []
        
        for symbol, position_info in self.stop_manager.positions.items():
            try:
                # 현재 종목 가격 조회
                if symbol != tick_data['symbol']:
                    current_data = await self.api_client.get_current_price(symbol)
                    if not current_data or current_data.get('rt_cd') != '0':
                        continue
                    current_price = int(current_data['output']['stck_prpr'])
                else:
                    current_price = tick_data['price']
                
                # 추적 손절 업데이트
                self.stop_manager.update_trailing_stop(symbol, current_price)
                
                # 매도 신호 체크
                exit_signal = self.stop_manager.check_exit_signal(symbol, current_price)
                
                if exit_signal:
                    exit_price, exit_reason = exit_signal
                    positions_to_exit.append((symbol, exit_price, exit_reason))
                    
            except Exception as e:
                self.logger.error(f"포지션 관리 오류 ({symbol}): {e}")
                
        # 매도 실행
        for symbol, exit_price, exit_reason in positions_to_exit:
            await self._execute_sell_order(symbol, exit_price, exit_reason)
            
    async def _check_new_buy_signals(self, tick_data: Dict[str, Any], market_condition: str):
        """신규 매수 신호 체크"""
        try:
            symbol = tick_data['symbol']
            
            # 과거 데이터 조회 (분석을 위해)
            price_data, volume_data = await self._get_historical_data(symbol)
            if not price_data or not volume_data:
                return
                
            # 매수 신호 분석
            should_buy, buy_reason = self.signal_analyzer.should_buy(
                price_data, volume_data, market_condition
            )
            
            if should_buy:
                # 포지션 사이즈 계산
                quantity = self.risk_manager.calculate_position_size(tick_data['price'])
                
                if quantity > 0:
                    await self._execute_buy_order(symbol, tick_data['price'], quantity, buy_reason)
                    
        except Exception as e:
            self.logger.error(f"매수 신호 체크 오류: {e}")
            
    async def _get_historical_data(self, symbol: str) -> Tuple[List[float], List[int]]:
        """과거 데이터 조회"""
        try:
            # 일봉 데이터 조회 (30일)
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
            
            result = await self.api_client.get_daily_price(symbol, start_date, end_date)
            
            if result and result.get('rt_cd') == '0':
                output = result.get('output2', [])
                
                prices = []
                volumes = []
                
                for day_data in output:
                    prices.append(int(day_data['stck_clpr']))  # 종가
                    volumes.append(int(day_data['acml_vol']))  # 누적거래량
                    
                return prices[-20:], volumes[-20:]  # 최근 20일 데이터
            
            return [], []
        except Exception as e:
            self.logger.error(f"과거 데이터 조회 오류: {e}")
            return [], []
            
    async def _execute_buy_order(self, symbol: str, price: int, quantity: int, reason: str):
        """매수 주문 실행"""
        try:
            self.logger.info(f"매수 주문 실행: {symbol}, 가격: {price:,}원, 수량: {quantity}주, 사유: {reason}")
            
            # 실제 주문 실행 (모의투자)
            result = await self.api_client.place_buy_order(
                symbol=symbol,
                quantity=quantity,
                price=price,
                order_type="00"  # 지정가
            )
            
            if result and result.get('rt_cd') == '0':
                self.logger.info(f"매수 주문 성공: {symbol}")
                
                # 포지션 추가
                self.stop_manager.add_position(symbol, price, quantity, datetime.now())
                
                # 텔레그램 알림 (주석 처리)
                # await self._send_telegram_notification(f"매수 완료: {symbol} {quantity}주 @ {price:,}원\n사유: {reason}")
                
            else:
                self.logger.error(f"매수 주문 실패: {symbol}, 결과: {result}")
                
        except Exception as e:
            self.logger.error(f"매수 주문 실행 오류: {e}")
            
    async def _execute_sell_order(self, symbol: str, price: int, reason: str):
        """매도 주문 실행"""
        try:
            position_info = self.stop_manager.positions.get(symbol)
            if not position_info:
                return
                
            quantity = position_info['quantity']
            entry_price = position_info['entry_price']
            entry_time = position_info['entry_time']
            
            self.logger.info(f"매도 주문 실행: {symbol}, 가격: {price:,}원, 수량: {quantity}주, 사유: {reason}")
            
            # 실제 주문 실행 (모의투자)
            result = await self.api_client.place_sell_order(
                symbol=symbol,
                quantity=quantity,
                price=price,
                order_type="00"  # 지정가
            )
            
            if result and result.get('rt_cd') == '0':
                self.logger.info(f"매도 주문 성공: {symbol}")
                
                # 매매 결과 기록
                self.trade_analyzer.add_trade_result(
                    symbol=symbol,
                    entry_price=entry_price,
                    exit_price=price,
                    quantity=quantity,
                    entry_time=entry_time,
                    exit_time=datetime.now(),
                    reason=reason
                )
                
                # 포지션 제거
                self.stop_manager.remove_position(symbol)
                
                # 손익 계산
                pnl = (price - entry_price) * quantity
                pnl_rate = (price - entry_price) / entry_price * 100
                
                # 텔레그램 알림 (주석 처리)
                # await self._send_telegram_notification(
                #     f"매도 완료: {symbol} {quantity}주 @ {price:,}원\n"
                #     f"손익: {pnl:,}원 ({pnl_rate:+.2f}%)\n사유: {reason}"
                # )
                
            else:
                self.logger.error(f"매도 주문 실패: {symbol}, 결과: {result}")
                
        except Exception as e:
            self.logger.error(f"매도 주문 실행 오류: {e}")
            
    async def _send_telegram_notification(self, message: str):
        """텔레그램 알림 전송 (주석 처리)"""
        # try:
        #     from ..utils import send_telegram_message
        #     await send_telegram_message(message, self.config)
        # except Exception as e:
        #     self.logger.error(f"텔레그램 알림 전송 오류: {e}")
        pass
        
    def stop_trading(self):
        """매매 중단"""
        self.is_running = False
        self.logger.info("Enhanced Trading System 중단 요청")
        
    def get_current_status(self) -> Dict[str, Any]:
        """현재 상태 조회"""
        return {
            'is_running': self.is_running,
            'positions': dict(self.stop_manager.positions),
            'risk_status': self.risk_manager.get_current_status(),
            'recent_performance': self.trade_analyzer.analyze_performance(7)
        }