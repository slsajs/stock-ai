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
from api_client import KISAPIClient
from utils import calculate_rsi, send_telegram_message, TradingConfig

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
        self.price_data: Dict[str, List[Dict]] = {}
        self.volume_history: Dict[str, List[int]] = {}
        self.target_stocks = ["005930", "000660", "035420", "005380", "051910"]  # 삼성전자, SK하이닉스, NAVER, 현대차, LG화학
        
        self.trading_hours = (time(9, 0), time(15, 30))
        self.is_trading_time = False
        
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
            available_cash = float(balance_data.get('output2', [{}])[0].get('dnca_tot_amt', 0))
            
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
        """실시간 데이터 처리"""
        try:
            logger.debug(f"Processing realtime data: {data}")
            
            if not self.is_market_hours():
                logger.debug("Market is closed, skipping data processing")
                return
            
            stock_code = data.get('mksc_shrn_iscd')
            logger.debug(f"Extracted stock_code: {stock_code}")
            
            if not stock_code:
                logger.debug("No stock_code found in data")
                return
                
            if stock_code not in self.target_stocks:
                logger.debug(f"Stock {stock_code} not in target list: {self.target_stocks}")
                return
            
            current_price = float(data.get('stck_prpr', 0))
            current_volume = int(data.get('cntg_vol', 0))
            
            logger.debug(f"Stock {stock_code} - Price: {current_price}, Volume: {current_volume}")
            
            if current_price == 0:
                logger.debug("Price is 0, skipping")
                return
            
            # 가격 데이터 업데이트
            logger.debug(f"Updating price data for {stock_code}")
            await self.update_price_data(stock_code, data)
            
            # 거래량 급증 확인
            volume_surge = await self.check_volume_surge(stock_code, current_volume)
            logger.debug(f"Volume surge detected for {stock_code}: {volume_surge}")
            
            # 손절/익절 확인 (기존 포지션)
            if stock_code in self.positions:
                logger.debug(f"Checking stop loss/take profit for existing position: {stock_code}")
                await self.check_stop_loss_take_profit(stock_code, current_price)
            
            # 새로운 진입 신호 확인
            if volume_surge:
                logger.debug(f"Volume surge detected, checking RSI signal for {stock_code}")
                rsi_signal = await self.calculate_rsi_signal(stock_code)
                logger.debug(f"RSI signal for {stock_code}: {rsi_signal}")
                
                if rsi_signal == "BUY" and stock_code not in self.positions:
                    logger.info(f"BUY signal generated for {stock_code}")
                    await self.execute_buy_order(stock_code, current_price)
                elif rsi_signal == "SELL" and stock_code in self.positions:
                    logger.info(f"SELL signal generated for {stock_code}")
                    await self.execute_sell_order(stock_code, current_price)
            
        except Exception as e:
            logger.error(f"Error processing realtime data: {e}")
            logger.debug(f"Data that caused error: {data}")
    
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
    
    async def start_trading(self):
        """자동 매매 시작"""
        logger.info("Auto trading started")
        
        try:
            while True:
                self.is_trading_time = self.is_market_hours()
                
                if not self.is_trading_time:
                    logger.info("Market closed, waiting...")
                    await asyncio.sleep(60)  # 1분 대기
                    continue
                
                # 포지션 상태 주기적 체크
                if self.positions:
                    summary = await self.get_positions_summary()
                    logger.info(summary)
                
                await asyncio.sleep(30)  # 30초 간격
                
        except KeyboardInterrupt:
            logger.info("Trading stopped by user")
        except Exception as e:
            logger.error(f"Trading loop error: {e}")
            await send_telegram_message(f"거래 시스템 오류: {e}", self.config)