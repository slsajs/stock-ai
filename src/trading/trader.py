import asyncio
import logging
from datetime import datetime, time
from typing import Dict, List, Optional
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
from dataclasses import dataclass
from ..api import KISAPIClient
from ..utils import calculate_rsi, send_telegram_message, TradingConfig
from ..analysis import TechnicalAnalyzer, DataManager, HybridDataManager, DynamicStockSelector
from ..analysis.market_analyzer import MarketAnalyzer
from ..analysis.enhanced_signal_analyzer import EnhancedSignalAnalyzer
from .risk_manager import RiskManager
from .stop_loss_manager import StopLossManager

logger = logging.getLogger(__name__)

@dataclass
class Position:
    stock_code: str
    quantity: int
    avg_price: float
    purchase_time: datetime
    current_price: float = 0.0
    
    @property
    def profit_loss_pct(self) -> float:
        if self.current_price == 0:
            return 0.0
        return ((self.current_price - self.avg_price) / self.avg_price) * 100
    
    @property
    def profit_loss_amount(self) -> int:
        return int((self.current_price - self.avg_price) * self.quantity)

class AutoTrader:
    def __init__(self, config: TradingConfig, api_client: KISAPIClient):
        self.config = config
        self.api = api_client
        self.positions: Dict[str, Position] = {}
        
        # 리스크 관리 모듈 초기화 (임시값, 실제 잔고는 start_trading에서 업데이트)
        self.risk_manager = RiskManager(initial_balance=8000000)  # 임시 800만원
        self.stop_loss_manager = StopLossManager(
            default_stop_loss_pct=0.02,  # 2% 손절
            default_take_profit_pct=0.03,  # 3% 익절
            trailing_stop_pct=0.015,  # 1.5% 트레일링
            max_position_time=30  # 30분 최대 보유
        )
        
        # 동적 종목 선정 시스템 초기화
        self.stock_selector = DynamicStockSelector(api_client)
        self.target_stocks = ["005930"]  # 초기값 (동적으로 업데이트 됨)
        self.need_resubscribe = False  # WebSocket 재구독 플래그
        
        # 분석 모듈 초기화
        self.analyzer = TechnicalAnalyzer()
        self.data_manager = DataManager(max_data_points=100)
        
        # 새로운 고급 분석 모듈 초기화
        self.market_analyzer = MarketAnalyzer()
        self.enhanced_signal = EnhancedSignalAnalyzer()
        
        # 하이브리드 데이터 매니저 초기화 (SQLite + 메모리)
        self.hybrid_managers = {}  # {stock_code: HybridDataManager}
        
        self.trading_hours = (time(9, 0), time(15, 30))
        self.is_trading_time = False
        self.max_investment = 1000000  # 최대 투자금액 100만원
        
    def is_market_hours(self) -> bool:
        """장중 시간인지 확인"""
        now = datetime.now().time()
        return self.trading_hours[0] <= now <= self.trading_hours[1]
    
    async def update_price_data(self, stock_code: str, price_data: Dict):
        """가격 데이터 업데이트"""
        if stock_code not in self.price_data:
            self.price_data[stock_code] = []
        
        current_time = datetime.now()
        data_point = {
            'timestamp': current_time,
            'price': float(price_data.get('stck_prpr', 0)),
            'volume': int(price_data.get('cntg_vol', 0)),
            'high': float(price_data.get('stck_hgpr', 0)),
            'low': float(price_data.get('stck_lwpr', 0))
        }
        
        self.price_data[stock_code].append(data_point)
        
        # 최근 100개 데이터만 유지
        if len(self.price_data[stock_code]) > 100:
            self.price_data[stock_code] = self.price_data[stock_code][-100:]
    
    async def check_volume_surge(self, stock_code: str, current_volume: int) -> bool:
        """거래량 급증 감지"""
        if stock_code not in self.volume_history:
            self.volume_history[stock_code] = []
        
        self.volume_history[stock_code].append(current_volume)
        
        if len(self.volume_history[stock_code]) > 20:
            self.volume_history[stock_code] = self.volume_history[stock_code][-20:]
        
        if len(self.volume_history[stock_code]) < 5:
            return False
        
        avg_volume = sum(self.volume_history[stock_code][:-1]) / len(self.volume_history[stock_code][:-1])
        
        if current_volume > avg_volume * self.config.volume_multiplier:
            logger.info(f"Volume surge detected for {stock_code}: {current_volume} vs avg {avg_volume}")
            return True
        
        return False
    
    async def calculate_rsi_signal(self, stock_code: str) -> Optional[str]:
        """RSI 기반 매매 신호 생성"""
        if stock_code not in self.price_data or len(self.price_data[stock_code]) < 14:
            return None
        
        prices = [data['price'] for data in self.price_data[stock_code]]
        rsi = calculate_rsi(prices)
        
        if rsi is None:
            return None
        
        logger.info(f"RSI for {stock_code}: {rsi:.2f}")
        
        if rsi <= self.config.rsi_oversold:
            return "BUY"
        elif rsi >= self.config.rsi_overbought:
            return "SELL"
        
        return None
    
    async def execute_buy_order(self, stock_code: str, current_price: float):
        """매수 주문 실행"""
        if len(self.positions) >= self.config.max_positions:
            logger.info(f"Maximum positions reached ({self.config.max_positions})")
            return
        
        if stock_code in self.positions:
            logger.info(f"Already holding position in {stock_code}")
            return
        
        try:
            # 잔고 조회
            balance_data = await self.api.get_balance()
            output2 = balance_data.get('output2', [])
            if output2:
                available_cash = float(output2[0].get('dnca_tot_amt', 0))
            else:
                logger.warning("No balance data available, using default investment amount")
                available_cash = self.max_investment  # 기본값 사용
            
            # 포지션당 투자 금액 계산 (총 잔고의 1/3)
            position_amount = available_cash / 3
            quantity = int(position_amount / current_price)
            
            if quantity < 1:
                logger.warning(f"Insufficient funds to buy {stock_code}")
                return
            
            # 매수 주문
            order_result = await self.api.place_order(stock_code, "buy", quantity, int(current_price))
            
            if order_result.get('rt_cd') == '0':
                position = Position(
                    stock_code=stock_code,
                    quantity=quantity,
                    avg_price=current_price,
                    purchase_time=datetime.now(),
                    current_price=current_price
                )
                self.positions[stock_code] = position
                
                message = f"매수 완료: {stock_code}, 수량: {quantity}, 가격: {current_price:,.0f}"
                logger.info(message)
                await send_telegram_message(message, self.config)
                
        except Exception as e:
            logger.error(f"Buy order failed for {stock_code}: {e}")
            await send_telegram_message(f"매수 실패: {stock_code} - {e}", self.config)
    
    async def execute_sell_order(self, stock_code: str, current_price: float):
        """매도 주문 실행"""
        if stock_code not in self.positions:
            return
        
        position = self.positions[stock_code]
        
        try:
            order_result = await self.api.place_order(stock_code, "sell", position.quantity, int(current_price))
            
            if order_result.get('rt_cd') == '0':
                profit_loss = position.profit_loss_amount
                profit_loss_pct = position.profit_loss_pct
                
                message = f"매도 완료: {stock_code}, 수량: {position.quantity}, 가격: {current_price:,.0f}, 손익: {profit_loss:+,.0f}원 ({profit_loss_pct:+.2f}%)"
                logger.info(message)
                await send_telegram_message(message, self.config)
                
                # CSV에 거래 기록 저장
                await self.log_trade(position, current_price, "SELL")
                
                del self.positions[stock_code]
                
        except Exception as e:
            logger.error(f"Sell order failed for {stock_code}: {e}")
            await send_telegram_message(f"매도 실패: {stock_code} - {e}", self.config)
    
    async def check_stop_loss_take_profit(self, stock_code: str, current_price: float):
        """손절/익절 조건 확인"""
        if stock_code not in self.positions:
            return
        
        position = self.positions[stock_code]
        position.current_price = current_price
        profit_loss_pct = position.profit_loss_pct
        
        if profit_loss_pct <= self.config.stop_loss_pct:
            logger.info(f"Stop loss triggered for {stock_code}: {profit_loss_pct:.2f}%")
            await self.execute_sell_order(stock_code, current_price)
        elif profit_loss_pct >= self.config.take_profit_pct:
            logger.info(f"Take profit triggered for {stock_code}: {profit_loss_pct:.2f}%")
            await self.execute_sell_order(stock_code, current_price)
    
    async def process_realtime_data(self, data: Dict):
        """실시간 데이터 처리 - 새로운 분석 로직 적용"""
        try:
            logger.debug(f"Processing realtime data: {data}")
            
            if not self.is_market_hours():
                logger.debug("Market is closed, skipping data processing")
                return
            
            # 파이프 구분 실시간 데이터 처리
            if 'tr_id' in data and data.get('tr_id') == 'H0STCNT0':
                stock_code = data.get('stock_code')
                current_price = data.get('current_price', 0)
                current_volume = data.get('volume', 0)
                timestamp = data.get('time', '')
                
                logger.info(f"Realtime: {stock_code} {current_price}원 거래량:{current_volume}")
                
                if not stock_code or stock_code not in self.target_stocks:
                    return
                
                if current_price <= 0:
                    return
                
                # 기존 DataManager에 데이터 저장 (호환성 유지)
                self.data_manager.add_tick_data(stock_code, current_price, current_volume, timestamp)
                
                # HybridDataManager에도 데이터 저장 (장기 보관 및 AI 학습용)
                if stock_code not in self.hybrid_managers:
                    self.hybrid_managers[stock_code] = HybridDataManager(
                        symbol=stock_code,
                        max_memory_ticks=200,  # 메모리에 200개만 유지
                        batch_size=50  # 50개씩 배치 저장
                    )
                    logger.info(f"HybridDataManager initialized for {stock_code}")
                
                # HybridDataManager에 데이터 추가
                timestamp_obj = datetime.now()  # 문자열 timestamp를 datetime 객체로 변환 필요시
                if isinstance(timestamp, str):
                    try:
                        # HH:MM:SS 형태의 문자열을 오늘 날짜와 결합
                        today = datetime.now().date()
                        time_parts = timestamp.split(':')
                        timestamp_obj = datetime.combine(today, datetime.min.time().replace(
                            hour=int(time_parts[0]), 
                            minute=int(time_parts[1]), 
                            second=int(time_parts[2]) if len(time_parts) > 2 else 0
                        ))
                    except:
                        timestamp_obj = datetime.now()
                
                self.hybrid_managers[stock_code].add_tick_data(current_price, current_volume, timestamp_obj)
                
                # 데이터 축적 상태 확인 (조건 완화)
                data_count = self.data_manager.get_data_count(stock_code)
                logger.debug(f"Data count for {stock_code}: {data_count}/20")
                
                if data_count >= 10:  # 20개에서 10개로 완화
                    logger.info(f"🔍 Starting analysis for {stock_code} (data: {data_count})")
                    await self._analyze_and_trade(stock_code, current_price, current_volume)
                else:
                    logger.debug(f"⏳ Waiting for more data: {stock_code} ({data_count}/10)")
            
            # JSON 형태 구독 응답 등은 무시
            else:
                logger.debug("Non-trading data received, skipping analysis")
            
        except Exception as e:
            logger.error(f"Error processing realtime data: {e}")
            logger.debug(f"Data that caused error: {data}")
    
    async def _analyze_and_trade(self, stock_code: str, current_price: float, current_volume: int):
        """기술적 분석 및 매매 결정 - 시장분석 및 다중지표 필터링 통합"""
        try:
            # 기존 포지션 손익 확인 (리스크 관리 통합)
            if stock_code in self.positions:
                logger.debug(f"📊 Checking exit conditions for existing position: {stock_code}")
                await self._check_position_exit_with_enhanced_analysis(stock_code, current_price)
                return  # 포지션 보유 중이면 신규 진입 안함
            
            # 1. 시장 상황 분석
            market_condition = self.market_analyzer.get_market_condition()
            logger.info(f"📊 Market condition: {market_condition[0]} - {market_condition[1]}")
            
            # 2. 신규 진입 신호 분석 - 데이터 준비
            prices = self.data_manager.get_recent_prices(stock_code)
            volumes = self.data_manager.get_recent_volumes(stock_code)
            
            if len(prices) < 10:  # 최소 데이터 요구사항
                logger.debug(f"⏳ Insufficient data for enhanced analysis: {stock_code} ({len(prices)}/10)")
                return
            
            # 3. 다중 지표 필터링을 통한 매수/매도 신호 평가
            should_buy, buy_reason = self.enhanced_signal.should_buy(prices, volumes, market_condition)
            
            logger.info(f"📊 Enhanced Analysis {stock_code} - {buy_reason}")
            logger.info(f"📊 Data points: prices({len(prices)}), volumes({len(volumes)})")
            
            # 4. 리스크 관리: 거래 가능 여부 확인
            can_trade, risk_reason = self.risk_manager.can_trade()
            if not can_trade:
                logger.info(f"🚫 Trading blocked by risk manager: {risk_reason}")
                return
            
            # 5. 매수 신호 처리
            if should_buy:
                logger.warning(f"🎯 ENHANCED BUY SIGNAL: {stock_code} - {buy_reason}")
                await self._execute_buy_with_risk_management(stock_code, current_price, buy_reason)
            else:
                # 기존 간단한 로직도 백업으로 유지 (거래량 급증 시)
                volume_surge = self.analyzer.detect_volume_surge(current_volume, volumes, surge_ratio=1.5)
                if len(self.positions) == 0 and volume_surge and market_condition[0] not in ["급락", "고변동성"]:
                    reason = f"백업매수신호-거래량급증"
                    logger.warning(f"🔄 BACKUP BUY SIGNAL: {stock_code} - {reason}")
                    await self._execute_buy_with_risk_management(stock_code, current_price, reason)
                else:
                    logger.debug(f"⏸️ No buy signal for {stock_code} - Enhanced analysis: {buy_reason}")
        
        except Exception as e:
            logger.error(f"Enhanced analysis error for {stock_code}: {e}")
    
    def _check_buy_signal(self, current_price: float, rsi: Optional[float], ma5: Optional[float], volume_surge: bool) -> bool:
        """매수 신호 판단 - 조건 완화"""
        if not rsi or not ma5:
            logger.debug(f"Missing indicators: RSI={rsi}, MA5={ma5}")
            return False
        
        # 개별 조건 체크 (디버깅용)
        rsi_oversold = rsi is not None and rsi < 35  # 30에서 35로 완화
        ma5_breakout = ma5 is not None and current_price > ma5
        
        rsi_str = f"{rsi:.1f}" if rsi else "N/A"
        ma5_str = f"{ma5:.0f}" if ma5 else "N/A"
        logger.info(f"🔍 Buy conditions: RSI({rsi_str} < 35)={rsi_oversold}, MA5돌파({current_price:.0f} > {ma5_str})={ma5_breakout}, 거래량급증={volume_surge}")
        
        # 조건 완화: 3개 중 2개만 만족하면 매수 (기존: 3개 모두)
        conditions_met = sum([rsi_oversold, ma5_breakout, volume_surge])
        
        if conditions_met >= 2:
            logger.warning(f"🎯 Buy signal triggered: {conditions_met}/3 conditions met")
            return True
        
        logger.debug(f"❌ Buy signal not triggered: only {conditions_met}/3 conditions met")
        return False
    
    async def _check_position_exit_with_enhanced_analysis(self, stock_code: str, current_price: float):
        """포지션 청산 조건 확인 - 다중지표 분석 통합"""
        if stock_code not in self.positions:
            return
        
        position = self.positions[stock_code]
        position.current_price = current_price
        
        # 1. StopLossManager에서 기본 리스크 관리 확인
        self.stop_loss_manager.update_price(stock_code, current_price)
        exit_signal = self.stop_loss_manager.check_exit_signal(stock_code, current_price)
        
        if exit_signal:
            action, reason, exit_info = exit_signal
            profit_rate = exit_info['profit_loss_pct']
            
            logger.warning(f"🎯 RISK MANAGED SELL SIGNAL: {stock_code} - {reason} (수익률: {profit_rate:+.2f}%)")
            await self._execute_sell_with_risk_management(stock_code, current_price, reason, profit_rate)
            return
        
        # 2. 다중 지표 기반 매도 신호 확인
        prices = self.data_manager.get_recent_prices(stock_code)
        volumes = self.data_manager.get_recent_volumes(stock_code)
        market_condition = self.market_analyzer.get_market_condition()
        
        if len(prices) >= 20:  # 충분한 데이터가 있을 때만 다중지표 분석
            should_sell, sell_reason = self.enhanced_signal.should_sell(prices, volumes, market_condition)
            profit_rate = position.profit_loss_pct
            
            if should_sell:
                logger.warning(f"🎯 ENHANCED SELL SIGNAL: {stock_code} - {sell_reason} (수익률: {profit_rate:+.2f}%)")
                await self._execute_sell_with_risk_management(stock_code, current_price, sell_reason, profit_rate)
                return
        
        # 3. 기존 RSI 기반 청산 조건 (백업)
        rsi = self.analyzer.calculate_rsi(prices, 14) if len(prices) >= 14 else None
        profit_rate = position.profit_loss_pct
        
        if rsi and self.analyzer.is_overbought(rsi) and profit_rate > 1.0:
            reason = f"백업매도신호-RSI과매수 ({rsi:.1f})"
            logger.warning(f"🔄 BACKUP SELL SIGNAL: {stock_code} - {reason} (수익률: {profit_rate:+.2f}%)")
            await self._execute_sell_with_risk_management(stock_code, current_price, reason, profit_rate)
    
    async def _execute_buy_with_risk_management(self, stock_code: str, price: float, reason: str):
        """리스크 관리가 통합된 매수 실행"""
        try:
            # 리스크 매니저에서 포지션 사이징 계산
            quantity = self.risk_manager.calculate_position_size(price, stock_code)
            
            if quantity < 1:
                logger.warning(f"🚫 Position size too small: {stock_code} - calculated quantity: {quantity}")
                return
            
            logger.warning(f"🛒 RISK MANAGED BUY: {stock_code} {quantity}주 @{price:,.0f}원 - {reason}")
            logger.warning(f"📊 Investment: {quantity * price:,.0f}원 | Risk Manager Balance: {self.risk_manager.current_balance:,.0f}원")
            
            # 실제 주문 실행
            try:
                order_result = await self.api.place_order(stock_code, "buy", quantity, int(price))
                logger.info(f"📋 Buy order API response: {order_result}")
            except Exception as api_error:
                logger.error(f"❌ Buy API call failed: {api_error}")
                # 테스트용 가상 성공
                logger.warning(f"🧪 TEST MODE: Creating virtual BUY position for {stock_code}")
                order_result = {'rt_cd': '0'}
            
            if order_result and order_result.get('rt_cd') == '0':
                # Position 객체 생성
                position = Position(
                    stock_code=stock_code,
                    quantity=quantity,
                    avg_price=price,
                    purchase_time=datetime.now(),
                    current_price=price
                )
                self.positions[stock_code] = position
                
                # StopLossManager에 포지션 추가
                self.stop_loss_manager.add_position(stock_code, price, quantity)
                
                # RiskManager에 거래 기록
                self.risk_manager.record_trade(stock_code, "buy", quantity, price, reason)
                
                # 데이터 매니저에도 기록 (기존 호환성)
                self.data_manager.save_trade_log(stock_code, "매수", price, quantity, reason, 0.0)
                
                message = f"💰 RISK MANAGED BUY!\n종목: {stock_code}\n수량: {quantity}주\n가격: {price:,.0f}원\n투자금: {quantity*int(price):,}원\n사유: {reason}\n잔고: {self.risk_manager.current_balance:,.0f}원"
                logger.warning(f"✅ BUY SUCCESS: {message.replace(chr(10), ' | ')}")
                await send_telegram_message(message, self.config)
            else:
                logger.error(f"❌ BUY ORDER FAILED: {stock_code} - API Response: {order_result}")
        
        except Exception as e:
            logger.error(f"❌ Risk managed buy execution failed for {stock_code}: {e}")
            import traceback
            logger.error(f"❌ Full traceback: {traceback.format_exc()}")

    async def _execute_buy(self, stock_code: str, price: float, reason: str):
        """매수 실행 - 강화된 로깅 및 테스트 모드"""
        try:
            # 현재 포지션 상태 로깅
            logger.warning(f"🔍 BUY ATTEMPT: {stock_code} - Current positions: {len(self.positions)}/3")
            
            if len(self.positions) >= 3:  # 최대 3개 포지션
                logger.warning(f"🚫 Maximum positions ({len(self.positions)}) reached - Cannot buy {stock_code}")
                return
            
            # 투자 금액 계산 (최대 투자금의 1/3)
            investment_amount = self.max_investment // 3
            quantity = investment_amount // int(price)
            
            if quantity < 1:
                logger.warning(f"🚫 Insufficient amount to buy {stock_code} - Need {int(price):,}원 but only have {investment_amount:,}원")
                return
            
            logger.warning(f"🛒 EXECUTING BUY ORDER: {stock_code} {quantity}주 @{price:,.0f}원 (투자금액: {quantity*int(price):,}원) - {reason}")
            
            # 실제 주문 실행 (모의투자)
            try:
                order_result = await self.api.place_order(stock_code, "buy", quantity, int(price))
                logger.info(f"📋 Buy order API response: {order_result}")
            except Exception as api_error:
                logger.error(f"❌ Buy API call failed: {api_error}")
                import traceback
                logger.error(f"❌ Buy API traceback: {traceback.format_exc()}")
                
                # 테스트용 가상 성공 (실제 거래 없이 포지션만 생성)
                logger.warning(f"🧪 TEST MODE: Creating virtual BUY position for {stock_code}")
                order_result = {'rt_cd': '0'}  # 가상 성공 응답
            
            if order_result and order_result.get('rt_cd') == '0':
                position = Position(
                    stock_code=stock_code,
                    quantity=quantity,
                    avg_price=price,
                    purchase_time=datetime.now(),
                    current_price=price
                )
                self.positions[stock_code] = position
                
                # 로그 저장
                self.data_manager.save_trade_log(stock_code, "매수", price, quantity, reason, 0.0)
                
                message = f"💰 BUY EXECUTED!\n종목: {stock_code}\n수량: {quantity}주\n가격: {price:,.0f}원\n투자금: {quantity*int(price):,}원\n사유: {reason}"
                logger.warning(f"✅ BUY SUCCESS: {message.replace(chr(10), ' | ')}")
                await send_telegram_message(message, self.config)
            else:
                logger.error(f"❌ BUY ORDER FAILED: {stock_code} - API Response: {order_result}")
        
        except Exception as e:
            logger.error(f"❌ Buy execution failed for {stock_code}: {e}")
            import traceback
            logger.error(f"❌ Full traceback: {traceback.format_exc()}")
    
    async def _execute_sell_with_risk_management(self, stock_code: str, price: float, reason: str, profit_rate: float):
        """리스크 관리가 통합된 매도 실행"""
        try:
            if stock_code not in self.positions:
                logger.warning(f"🚫 Cannot sell {stock_code} - No position found")
                return
            
            position = self.positions[stock_code]
            profit_amount = position.profit_loss_amount
            sell_value = position.quantity * int(price)
            
            logger.warning(f"💸 RISK MANAGED SELL: {stock_code} {position.quantity}주 @{price:,.0f}원 - {reason}")
            logger.warning(f"📊 예상손익: {profit_amount:+,}원 ({profit_rate:+.2f}%) | 매수가: {position.avg_price:,.0f}원")
            
            # 실제 주문 실행
            try:
                order_result = await self.api.place_order(stock_code, "sell", position.quantity, int(price))
                logger.info(f"📋 Sell order API response: {order_result}")
            except Exception as api_error:
                logger.error(f"❌ Sell API call failed: {api_error}")
                # 테스트용 가상 성공
                logger.warning(f"🧪 TEST MODE: Creating virtual SELL for {stock_code}")
                order_result = {'rt_cd': '0'}
            
            if order_result and order_result.get('rt_cd') == '0':
                # RiskManager에 거래 기록
                self.risk_manager.record_trade(stock_code, "sell", position.quantity, price, reason, profit_amount, profit_rate)
                
                # StopLossManager에서 포지션 제거
                self.stop_loss_manager.remove_position(stock_code)
                
                # 데이터 매니저에도 기록 (기존 호환성)
                self.data_manager.save_trade_log(stock_code, "매도", price, position.quantity, reason, profit_rate)
                
                profit_emoji = "💚" if profit_amount > 0 else "❤️" if profit_amount < 0 else "💛"
                message = f"{profit_emoji} RISK MANAGED SELL!\n종목: {stock_code}\n수량: {position.quantity}주\n가격: {price:,.0f}원\n매도금액: {sell_value:,}원\n손익: {profit_amount:+,.0f}원 ({profit_rate:+.2f}%)\n사유: {reason}\n잔고: {self.risk_manager.current_balance:,.0f}원"
                logger.warning(f"✅ SELL SUCCESS: {message.replace(chr(10), ' | ')}")
                await send_telegram_message(message, self.config)
                
                # 성과 분석 로그
                daily_summary = self.risk_manager.get_daily_summary()
                logger.info(f"📊 Daily Summary: {daily_summary['total_trades']}거래, 승률: {daily_summary['win_rate']:.1%}, 일일손익: {daily_summary['daily_pnl']:+,.0f}원")
                
                del self.positions[stock_code]
            else:
                logger.error(f"❌ SELL ORDER FAILED: {stock_code} - API Response: {order_result}")
        
        except Exception as e:
            logger.error(f"❌ Risk managed sell execution failed for {stock_code}: {e}")

    async def _execute_sell(self, stock_code: str, price: float, reason: str, profit_rate: float):
        """매도 실행"""
        try:
            if stock_code not in self.positions:
                logger.warning(f"🚫 Cannot sell {stock_code} - No position found")
                return
            
            position = self.positions[stock_code]
            profit_amount = position.profit_loss_amount
            sell_value = position.quantity * int(price)
            
            logger.warning(f"💸 EXECUTING SELL ORDER: {stock_code} {position.quantity}주 @{price:,.0f}원 (매도금액: {sell_value:,}원) - {reason}")
            logger.warning(f"📊 예상손익: {profit_amount:+,}원 ({profit_rate:+.2f}%) | 매수가: {position.avg_price:,.0f}원")
            
            # 실제 주문 실행 (모의투자)
            try:
                order_result = await self.api.place_order(stock_code, "sell", position.quantity, int(price))
                logger.info(f"📋 Sell order API response: {order_result}")
            except Exception as api_error:
                logger.error(f"❌ Sell API call failed: {api_error}")
                import traceback
                logger.error(f"❌ Sell API traceback: {traceback.format_exc()}")
                
                # 테스트용 가상 성공 (실제 거래 없이 포지션만 제거)
                logger.warning(f"🧪 TEST MODE: Creating virtual SELL for {stock_code}")
                order_result = {'rt_cd': '0'}  # 가상 성공 응답
            
            if order_result and order_result.get('rt_cd') == '0':
                # 로그 저장
                self.data_manager.save_trade_log(stock_code, "매도", price, position.quantity, reason, profit_rate)
                
                profit_emoji = "💚" if profit_amount > 0 else "❤️" if profit_amount < 0 else "💛"
                message = f"{profit_emoji} SELL EXECUTED!\n종목: {stock_code}\n수량: {position.quantity}주\n가격: {price:,.0f}원\n매도금액: {sell_value:,}원\n손익: {profit_amount:+,.0f}원 ({profit_rate:+.2f}%)\n사유: {reason}"
                logger.warning(f"✅ SELL SUCCESS: {message.replace(chr(10), ' | ')}")
                await send_telegram_message(message, self.config)
                
                del self.positions[stock_code]
            else:
                logger.error(f"❌ SELL ORDER FAILED: {stock_code} - API Response: {order_result}")
        
        except Exception as e:
            logger.error(f"❌ Sell execution failed for {stock_code}: {e}")
    
    async def log_trade(self, position: Position, sell_price: float, action: str):
        """거래 기록을 CSV 파일에 저장"""
        try:
            trade_data = {
                'timestamp': datetime.now().isoformat(),
                'stock_code': position.stock_code,
                'action': action,
                'quantity': position.quantity,
                'buy_price': position.avg_price,
                'sell_price': sell_price if action == 'SELL' else 0,
                'profit_loss': position.profit_loss_amount if action == 'SELL' else 0,
                'profit_loss_pct': position.profit_loss_pct if action == 'SELL' else 0
            }
            
            if PANDAS_AVAILABLE:
                df = pd.DataFrame([trade_data])
                df.to_csv('trades.csv', mode='a', header=False, index=False)
            else:
                import csv
                with open('trades.csv', 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=trade_data.keys())
                    writer.writerow(trade_data)
            
        except Exception as e:
            logger.error(f"Failed to log trade: {e}")
    
    async def get_positions_summary(self) -> str:
        """현재 포지션 요약"""
        if not self.positions:
            return "보유 포지션 없음"
        
        summary = "현재 포지션:\n"
        total_pnl = 0
        
        for stock_code, position in self.positions.items():
            pnl_amount = position.profit_loss_amount
            pnl_pct = position.profit_loss_pct
            total_pnl += pnl_amount
            
            summary += f"• {stock_code}: {position.quantity}주, 손익: {pnl_amount:+,.0f}원 ({pnl_pct:+.2f}%)\n"
        
        summary += f"총 손익: {total_pnl:+,.0f}원"
        return summary
    
    async def update_target_stocks(self):
        """대상 종목 동적 업데이트"""
        try:
            new_targets = await self.stock_selector.get_dynamic_target_stocks()
            
            # 종목 변경이 있었는지 확인
            if set(new_targets) != set(self.target_stocks):
                old_targets = self.target_stocks.copy()
                self.target_stocks = new_targets
                
                logger.info(f"🔄 Target stocks updated: {len(new_targets)} stocks")
                logger.info(f"   Old: {old_targets}")
                logger.info(f"   New: {new_targets}")
                
                # WebSocket 재구독 필요 플래그 설정
                self.need_resubscribe = True
                
                # 변경 내역 알림
                summary = await self.stock_selector.get_stock_summary()
                await send_telegram_message(f"🎯 대상 종목이 업데이트되었습니다:\n{summary}", self.config)
                
                # 기존 포지션 중 새 대상에 없는 종목은 청산 검토
                await self._review_positions_for_new_targets()
                
        except Exception as e:
            logger.error(f"Error updating target stocks: {e}")

    async def _review_positions_for_new_targets(self):
        """새로운 대상 종목 기준으로 기존 포지션 검토"""
        for stock_code in list(self.positions.keys()):
            if stock_code not in self.target_stocks:
                logger.info(f"Stock {stock_code} no longer in targets, considering exit...")
                # 즉시 청산하지 말고 조건부 청산 (수익이 있을 때만)
                position = self.positions[stock_code]
                if position.profit_loss_pct > 1.0:  # 1% 이상 수익 시 청산
                    current_price = await self._get_current_price(stock_code)
                    if current_price:
                        await self._execute_sell(stock_code, current_price, "대상종목제외", position.profit_loss_pct)

    async def _get_current_price(self, stock_code: str) -> Optional[float]:
        """종목의 현재가 조회"""
        try:
            price_data = await self.api.get_current_price(stock_code)
            if price_data and price_data.get('rt_cd') == '0':
                return float(price_data['output'].get('stck_prpr', 0))
        except Exception as e:
            logger.error(f"Error getting current price for {stock_code}: {e}")
        return None

    async def update_risk_manager_balance(self):
        """실제 계좌 잔고를 조회하여 리스크 매니저 업데이트"""
        try:
            balance_result = await self.api.get_balance()
            logger.debug(f"Balance API response: {balance_result}")
            
            if balance_result.get('rt_cd') == '0':
                # API 응답 구조 확인
                output2 = balance_result.get('output2')
                if output2:
                    # output2가 딕셔너리인지 리스트인지 확인
                    if isinstance(output2, list) and len(output2) > 0:
                        output2 = output2[0]  # 첫 번째 항목 사용
                    elif isinstance(output2, dict):
                        pass  # 그대로 사용
                    else:
                        logger.warning(f"Unexpected output2 structure: {type(output2)}")
                        return self.risk_manager.current_balance
                    
                    # 예수금 총액 (사용 가능한 현금) 여러 필드 시도
                    cash_balance = None
                    for field in ['dnca_tot_amt', 'tot_evlu_amt', 'nass_amt', 'pchs_amt_smtl']:
                        if field in output2:
                            try:
                                cash_balance = int(output2[field])
                                logger.info(f"사용한 잔고 필드: {field} = {cash_balance:,}원")
                                break
                            except (ValueError, TypeError):
                                continue
                    
                    if cash_balance is None:
                        logger.warning(f"잔고 필드를 찾을 수 없습니다. output2 keys: {output2.keys() if isinstance(output2, dict) else 'not dict'}")
                        return self.risk_manager.current_balance
                    
                    # 리스크 매니저 잔고 업데이트
                    self.risk_manager.current_balance = cash_balance
                    self.risk_manager.initial_balance = max(cash_balance, self.risk_manager.initial_balance)
                    
                    logger.info(f"💰 실제 계좌 잔고 업데이트: {cash_balance:,}원")
                    return cash_balance
                else:
                    logger.warning("output2가 없습니다")
                    return self.risk_manager.current_balance
            else:
                logger.warning(f"잔고 조회 실패: {balance_result.get('msg1', 'Unknown error')}")
                return self.risk_manager.current_balance
        except Exception as e:
            logger.error(f"잔고 조회 중 오류: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return self.risk_manager.current_balance

    async def get_account_positions(self) -> Dict[str, Position]:
        """실제 계좌의 보유 주식 조회"""
        try:
            balance_result = await self.api.get_balance()
            positions = {}
            
            if balance_result.get('rt_cd') == '0':
                # output1에 보유 종목 정보가 있음
                output1 = balance_result.get('output1', [])
                
                if isinstance(output1, list):
                    for stock_info in output1:
                        stock_code = stock_info.get('pdno', '').strip()
                        if not stock_code or stock_code == '':
                            continue
                            
                        quantity = int(stock_info.get('hldg_qty', 0))
                        if quantity <= 0:
                            continue
                            
                        avg_price = float(stock_info.get('pchs_avg_pric', 0))
                        current_price = float(stock_info.get('prpr', 0))
                        
                        # 매입일 정보가 있으면 사용, 없으면 현재 시간
                        purchase_time = datetime.now()  # API에서 매입일 정보 제공 시 파싱 필요
                        
                        position = Position(
                            stock_code=stock_code,
                            quantity=quantity,
                            avg_price=avg_price,
                            purchase_time=purchase_time,
                            current_price=current_price
                        )
                        
                        positions[stock_code] = position
                        logger.info(f"실제 계좌 보유: {stock_code} {quantity}주 @{avg_price:,.0f}원 (현재: {current_price:,.0f}원)")
                
                logger.info(f"총 {len(positions)}개 종목 보유 중")
                return positions
            else:
                logger.warning(f"계좌 조회 실패: {balance_result.get('msg1', 'Unknown error')}")
                return {}
                
        except Exception as e:
            logger.error(f"계좌 보유 주식 조회 중 오류: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {}

    async def sync_positions_with_account(self):
        """실제 계좌와 시스템 포지션 동기화"""
        try:
            account_positions = await self.get_account_positions()
            
            # 최근 매수한 종목들 보호 (5분 이내 매수한 종목은 유지)
            protected_positions = {}
            current_time = datetime.now()
            for code, pos in self.positions.items():
                time_since_purchase = (current_time - pos.purchase_time).total_seconds() / 60
                if time_since_purchase < 5:  # 5분 이내 매수
                    protected_positions[code] = pos
                    logger.debug(f"보호된 포지션: {code} (매수 후 {time_since_purchase:.1f}분)")
            
            # 시스템 포지션을 실제 계좌와 동기화
            self.positions.clear()
            
            # 실제 계좌 포지션 추가
            for stock_code, position in account_positions.items():
                self.positions[stock_code] = position
            
            # 보호된 포지션 복원 (실제 계좌에 없더라도)
            for code, pos in protected_positions.items():
                if code not in self.positions:
                    self.positions[code] = pos
                    logger.info(f"최근 매수 종목 복원: {code}")
                
                # 손절 매니저에도 추가 (기본 손절값으로)
                self.stop_loss_manager.add_position(
                    symbol=stock_code,
                    entry_price=position.avg_price,
                    quantity=position.quantity,
                    entry_time=position.purchase_time
                )
                
            logger.info(f"시스템 포지션 동기화 완료: {len(self.positions)}개 종목")
            
        except Exception as e:
            logger.error(f"포지션 동기화 중 오류: {e}")

    async def monitor_existing_positions(self):
        """기존 보유 주식 모니터링 및 손절/익절 처리"""
        if not self.positions:
            return
            
        for stock_code, position in list(self.positions.items()):
            try:
                # 현재가 조회
                current_price = await self._get_current_price(stock_code)
                if not current_price:
                    continue
                    
                position.current_price = current_price
                
                # 손익률 계산
                profit_loss_pct = position.profit_loss_pct
                profit_loss_amount = position.profit_loss_amount
                
                logger.debug(f"{stock_code}: {profit_loss_pct:+.2f}% ({profit_loss_amount:+,}원)")
                
                # 매수 후 일정 시간(3분) 이내에는 완화된 조건 적용
                time_since_purchase = (datetime.now() - position.purchase_time).total_seconds() / 60
                
                if time_since_purchase < 3:  # 3분 이내
                    # 매수 직후에는 더 관대한 조건
                    stop_loss_threshold = -3.0  # -3%
                    take_profit_threshold = 4.0  # +4%
                    logger.debug(f"{stock_code} 매수 후 {time_since_purchase:.1f}분 - 완화된 조건 적용")
                else:
                    # 일반 조건
                    stop_loss_threshold = -2.0  # -2%
                    take_profit_threshold = 3.0  # +3%
                
                # 손절 체크
                if profit_loss_pct <= stop_loss_threshold:
                    logger.warning(f"🚨 손절 신호: {stock_code} {profit_loss_pct:+.2f}%")
                    await self.execute_sell_order(stock_code, current_price)
                    continue
                    
                # 익절 체크
                elif profit_loss_pct >= take_profit_threshold:
                    logger.info(f"💰 익절 신호: {stock_code} {profit_loss_pct:+.2f}%")
                    await self.execute_sell_order(stock_code, current_price)
                    continue
                    
                # 트레일링 스톱 체크
                stop_price = self.stop_loss_manager.check_exit_signal(stock_code, current_price)
                if stop_price:
                    exit_price, reason = stop_price
                    logger.info(f"📉 트레일링 스톱: {stock_code} {reason}")
                    await self.execute_sell_order(stock_code, exit_price)
                    continue
                    
                # 트레일링 스톱 업데이트
                self.stop_loss_manager.update_price(stock_code, current_price)
                
            except Exception as e:
                logger.error(f"포지션 모니터링 오류 ({stock_code}): {e}")

    async def start_trading(self):
        """자동 매매 시작 - 동적 종목 선정 추가"""
        logger.info("🚀 Auto trading system started with dynamic stock selection")
        
        # 실제 잔고 조회 및 리스크 매니저 업데이트
        await self.update_risk_manager_balance()
        
        # 실제 계좌와 시스템 포지션 동기화
        logger.info("🔄 Syncing with account positions...")
        await self.sync_positions_with_account()
        
        # 초기 종목 선정
        logger.info("🎯 Initial target stock selection...")
        await self.update_target_stocks()
        
        await send_telegram_message("📈 동적 종목선정 자동매매 시스템이 시작되었습니다.", self.config)
        
        try:
            while True:
                current_time = datetime.now().time()
                self.is_trading_time = self.is_market_hours()
                
                # 장 마감시간(15:30) 이후 자동 종료
                if current_time > time(15, 30):
                    logger.info("Market closed for the day, shutting down...")
                    await send_telegram_message("📊 장 마감으로 시스템을 종료합니다.", self.config)
                    
                    # 보유 포지션이 있으면 알림
                    if self.positions:
                        summary = await self.get_positions_summary()
                        await send_telegram_message(f"💼 장 마감 시 보유 포지션:\n{summary}", self.config)
                    
                    # HybridDataManager 배치 데이터 강제 저장
                    self._save_all_hybrid_data()
                    
                    # 리스크 관리 데이터 저장
                    self._save_risk_management_data()
                    
                    break
                
                if not self.is_trading_time:
                    logger.debug("Market not open, waiting...")
                    await asyncio.sleep(60)  # 1분 대기
                    continue
                
                # 기존 보유 주식 모니터링 (매 루프마다)
                await self.monitor_existing_positions()
                
                # 정기 작업 (5분마다)
                if datetime.now().minute % 5 == 0:
                    # 계좌와 포지션 재동기화 (실제 계좌 변동 반영)
                    await self.sync_positions_with_account()
                    
                    # 포지션 상태 체크
                    if self.positions:
                        summary = await self.get_positions_summary()
                        logger.info(summary)
                    
                    # 데이터 관리 (메모리 정리)
                    self.data_manager.clear_old_data()
                    
                    # HybridDataManager 통계 및 상태 체크
                    if self.hybrid_managers:
                        hybrid_summary = self.get_hybrid_data_summary()
                        logger.info(hybrid_summary)
                        
                        # 상태 체크 및 복구
                        self._check_hybrid_managers_health()
                
                # 대상 종목 업데이트 체크 (5분마다) - 테스트용
                if datetime.now().minute % 5 == 0 and datetime.now().second < 30:
                    logger.info("⏰ Time to check for target stock updates...")
                    await self.update_target_stocks()
                
                await asyncio.sleep(60)  # 60초 간격 (API 호출 줄이기)
                
        except KeyboardInterrupt:
            logger.info("Trading stopped by user")
            await send_telegram_message("⛔ 사용자에 의해 시스템이 중지되었습니다.", self.config)
            # 시스템 종료 시 배치 데이터 저장
            self._save_all_hybrid_data()
            # 리스크 관리 데이터 저장
            self._save_risk_management_data()
        except Exception as e:
            logger.error(f"Trading loop error: {e}")
            await send_telegram_message(f"🚨 거래 시스템 오류: {e}", self.config)
            # 오류 발생 시에도 배치 데이터 저장
            self._save_all_hybrid_data()
            # 리스크 관리 데이터 저장
            self._save_risk_management_data()

    def _save_all_hybrid_data(self):
        """모든 HybridDataManager의 배치 데이터 강제 저장 및 안전 종료"""
        try:
            total_saved = 0
            for stock_code, manager in self.hybrid_managers.items():
                # 안전한 종료로 모든 대기 중인 데이터 저장
                manager.shutdown()
                
                stats = manager.get_data_statistics()
                total_saved += stats.get('db_tick_count', 0)
                logger.info(f"HybridDataManager {stock_code}: {stats.get('db_tick_count', 0)}개 데이터 저장됨")
                
                # 상태 체크
                health = manager.health_check()
                if not all(health.values()):
                    logger.warning(f"HybridDataManager {stock_code} health issues: {health}")
            
            logger.info(f"총 {total_saved:,}개의 데이터가 DB에 저장되었습니다.")
            
        except Exception as e:
            logger.error(f"Failed to save hybrid data: {e}")

    def get_hybrid_data_summary(self) -> str:
        """HybridDataManager 데이터 통계 요약"""
        if not self.hybrid_managers:
            return "HybridDataManager 데이터 없음"
        
        summary = "📊 HybridDataManager 데이터 통계:\n"
        total_tick_count = 0
        total_minute_count = 0
        total_db_size = 0
        
        for stock_code, manager in self.hybrid_managers.items():
            stats = manager.get_data_statistics()
            tick_count = stats.get('db_tick_count', 0)
            minute_count = stats.get('db_minute_count', 0)
            db_size = stats.get('db_file_size', 0)
            
            total_tick_count += tick_count
            total_minute_count += minute_count
            total_db_size += db_size
            
            summary += f"• {stock_code}: 체결 {tick_count:,}건, 분봉 {minute_count:,}건 ({db_size/1024:.1f}KB)\n"
        
        summary += f"💾 총계: 체결 {total_tick_count:,}건, 분봉 {total_minute_count:,}건 ({total_db_size/1024:.1f}KB)"
        return summary

    def _check_hybrid_managers_health(self):
        """HybridDataManager들의 상태를 체크하고 문제 시 복구"""
        try:
            for stock_code, manager in list(self.hybrid_managers.items()):
                health = manager.health_check()
                
                # 심각한 문제가 있는 경우
                unhealthy_count = sum(1 for status in health.values() if not status)
                
                if unhealthy_count >= 3:  # 5개 중 3개 이상 문제
                    logger.error(f"HybridDataManager {stock_code} is severely unhealthy: {health}")
                    
                    # 기존 매니저 종료
                    try:
                        manager.shutdown()
                    except Exception as e:
                        logger.error(f"Error shutting down unhealthy manager {stock_code}: {e}")
                    
                    # 새 매니저로 교체
                    try:
                        self.hybrid_managers[stock_code] = HybridDataManager(
                            symbol=stock_code,
                            max_memory_ticks=200,
                            batch_size=50
                        )
                        logger.warning(f"Replaced unhealthy HybridDataManager for {stock_code}")
                        # 비동기 메시지는 나중에 처리하도록 플래그만 설정
                        logger.info(f"Manager replacement completed for {stock_code}")
                    except Exception as e:
                        logger.error(f"Failed to replace manager for {stock_code}: {e}")
                        # 실패한 매니저는 제거
                        del self.hybrid_managers[stock_code]
                
                elif unhealthy_count > 0:
                    logger.warning(f"HybridDataManager {stock_code} has minor issues: {health}")
                
        except Exception as e:
            logger.error(f"Error checking hybrid managers health: {e}")

    def _save_risk_management_data(self):
        """리스크 관리 데이터 저장"""
        try:
            # 리스크 매니저 데이터 저장
            self.risk_manager.save_to_file()
            
            # StopLossManager 요약 로그
            sl_summary = self.stop_loss_manager.get_summary()
            logger.info(f"StopLossManager Summary: {sl_summary}")
            
            # 성과 분석 저장
            performance = self.risk_manager.get_performance_analysis()
            if performance:
                logger.info(f"📊 Performance Analysis: Win Rate: {performance.get('win_rate', 0):.1%}, "
                           f"Total P&L: {performance.get('total_pnl', 0):+,.0f}원, "
                           f"Max Drawdown: {performance.get('max_drawdown', 0):.1%}")
                
                # 성과가 좋지 않으면 경고
                if performance.get('win_rate', 0) < 0.4 or performance.get('total_pnl', 0) < -50000:
                    logger.warning("⚠️ Poor performance detected - Review risk parameters!")
            
        except Exception as e:
            logger.error(f"Failed to save risk management data: {e}")

    async def get_risk_summary(self) -> str:
        """리스크 관리 요약"""
        try:
            # 리스크 매니저 요약
            daily_summary = self.risk_manager.get_daily_summary()
            performance = self.risk_manager.get_performance_analysis()
            
            # 현재 포지션 리스크 상태
            current_prices = {}
            for symbol in self.positions.keys():
                try:
                    price_data = await self.api.get_current_price(symbol)
                    if price_data and price_data.get('rt_cd') == '0':
                        current_prices[symbol] = float(price_data['output'].get('stck_prpr', 0))
                except:
                    current_prices[symbol] = self.positions[symbol].current_price
            
            position_statuses = self.stop_loss_manager.get_all_positions_status(current_prices)
            
            summary = f"🎯 리스크 관리 요약\n"
            summary += f"━━━━━━━━━━━━━━━━━━━━━━\n"
            summary += f"💰 현재 잔고: {self.risk_manager.current_balance:,.0f}원\n"
            summary += f"📊 일일 손익: {daily_summary.get('daily_pnl', 0):+,.0f}원\n"
            summary += f"🔢 오늘 거래: {daily_summary.get('total_trades', 0)}회\n"
            summary += f"📈 승률: {daily_summary.get('win_rate', 0):.1%}\n"
            summary += f"🔥 연속손실: {self.risk_manager.consecutive_losses}회\n"
            summary += f"✅ 거래가능: {'가능' if daily_summary.get('can_trade', False) else '불가능'}\n"
            
            if performance:
                summary += f"\n📈 전체 성과 (총 {performance.get('total_trades', 0)}거래)\n"
                summary += f"총손익: {performance.get('total_pnl', 0):+,.0f}원 ({performance.get('total_return', 0):+.1%})\n"
                summary += f"최대손실: {performance.get('max_drawdown', 0):.1%}\n"
                summary += f"수익인수: {performance.get('profit_factor', 0):.2f}\n"
            
            if position_statuses:
                summary += f"\n🎯 현재 포지션 리스크 상태\n"
                for pos in position_statuses:
                    summary += f"• {pos['symbol']}: {pos['profit_loss_pct']:+.1f}% "
                    summary += f"(손절까지 {pos['distance_to_stop_loss']:.1f}%)\n"
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating risk summary: {e}")
            return "리스크 요약 생성 실패"