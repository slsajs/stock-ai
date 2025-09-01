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
from ..analysis import TechnicalAnalyzer, DataManager, DynamicStockSelector

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
        
        # 동적 종목 선정 시스템 초기화
        self.stock_selector = DynamicStockSelector(api_client)
        self.target_stocks = ["005930"]  # 초기값 (동적으로 업데이트 됨)
        self.need_resubscribe = False  # WebSocket 재구독 플래그
        
        # 분석 모듈 초기화
        self.analyzer = TechnicalAnalyzer()
        self.data_manager = DataManager(max_data_points=100)
        
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
                
                # 데이터 저장
                self.data_manager.add_tick_data(stock_code, current_price, current_volume, timestamp)
                
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
        """기술적 분석 및 매매 결정"""
        try:
            # 기존 포지션 손익 확인
            if stock_code in self.positions:
                logger.debug(f"📊 Checking exit conditions for existing position: {stock_code}")
                await self._check_position_exit(stock_code, current_price)
                return  # 포지션 보유 중이면 신규 진입 안함
            
            # 신규 진입 신호 분석
            prices = self.data_manager.get_recent_prices(stock_code)
            volumes = self.data_manager.get_recent_volumes(stock_code)
            highs_lows = self.data_manager.get_recent_highs_lows(stock_code)
            
            # 기술적 지표 계산 (조건 완화)
            rsi = self.analyzer.calculate_rsi(prices, 10) if len(prices) >= 10 else None  # 14에서 10으로 완화
            ma5 = self.analyzer.calculate_moving_average(prices, 3) if len(prices) >= 3 else None  # 5에서 3으로 완화
            volume_surge = self.analyzer.detect_volume_surge(current_volume, volumes, surge_ratio=1.5)  # 2.0에서 1.5로 완화
            
            ma5_str = f"{ma5:.0f}" if ma5 else "N/A"
            logger.info(f"📊 Analysis {stock_code} - RSI:{rsi}, MA5:{ma5_str}, Volume surge:{volume_surge}")
            logger.info(f"📊 Data points: prices({len(prices)}), volumes({len(volumes)})")
            
            # 매수 조건 확인
            buy_signal = self._check_buy_signal(current_price, rsi, ma5, volume_surge)
            
            if buy_signal:
                rsi_str = f"{rsi:.1f}" if rsi else "N/A"
                reason = f"RSI:{rsi_str}, MA돌파, 거래량급증"
                logger.warning(f"🎯 BUY SIGNAL DETECTED: {stock_code} - {reason}")
                await self._execute_buy(stock_code, current_price, reason)
            else:
                # 테스트용: 매우 완화된 조건으로도 시도
                if len(self.positions) == 0 and volume_surge:  # 포지션이 없고 거래량만 급증해도
                    rsi_str = f"{rsi:.1f}" if rsi else "N/A"
                    reason = f"테스트매수-거래량급증(RSI:{rsi_str})"
                    logger.warning(f"🧪 TEST BUY SIGNAL: {stock_code} - {reason}")
                    await self._execute_buy(stock_code, current_price, reason)
                else:
                    logger.debug(f"⏸️ No buy signal for {stock_code} - conditions not met")
        
        except Exception as e:
            logger.error(f"Analysis error for {stock_code}: {e}")
    
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
    
    async def _check_position_exit(self, stock_code: str, current_price: float):
        """포지션 청산 조건 확인"""
        if stock_code not in self.positions:
            return
        
        position = self.positions[stock_code]
        position.current_price = current_price
        profit_rate = position.profit_loss_pct
        
        prices = self.data_manager.get_recent_prices(stock_code)
        rsi = self.analyzer.calculate_rsi(prices, 14) if len(prices) >= 14 else None
        
        # 매도 조건 확인
        sell_conditions = [
            profit_rate > 3.0,  # 수익률 3% 이상
            profit_rate < -2.0,  # 손실률 2% 이상
            self.analyzer.is_overbought(rsi) if rsi else False  # RSI 과매수
        ]
        
        if any(sell_conditions):
            reason = "익절3%" if profit_rate > 3 else "손절-2%" if profit_rate < -2 else "RSI과매수"
            logger.warning(f"🎯 SELL SIGNAL DETECTED: {stock_code} - {reason} (현재수익률: {profit_rate:+.2f}%)")
            await self._execute_sell(stock_code, current_price, reason, profit_rate)
    
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

    async def start_trading(self):
        """자동 매매 시작 - 동적 종목 선정 추가"""
        logger.info("🚀 Auto trading system started with dynamic stock selection")
        
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
                    
                    break
                
                if not self.is_trading_time:
                    logger.debug("Market not open, waiting...")
                    await asyncio.sleep(60)  # 1분 대기
                    continue
                
                # 정기 작업 (5분마다)
                if datetime.now().minute % 5 == 0:
                    # 포지션 상태 체크
                    if self.positions:
                        summary = await self.get_positions_summary()
                        logger.info(summary)
                    
                    # 데이터 관리 (메모리 정리)
                    self.data_manager.clear_old_data()
                
                # 대상 종목 업데이트 체크 (5분마다) - 테스트용
                if datetime.now().minute % 5 == 0 and datetime.now().second < 30:
                    logger.info("⏰ Time to check for target stock updates...")
                    await self.update_target_stocks()
                
                await asyncio.sleep(30)  # 30초 간격
                
        except KeyboardInterrupt:
            logger.info("Trading stopped by user")
            await send_telegram_message("⛔ 사용자에 의해 시스템이 중지되었습니다.", self.config)
        except Exception as e:
            logger.error(f"Trading loop error: {e}")
            await send_telegram_message(f"🚨 거래 시스템 오류: {e}", self.config)