import logging
import csv
import os
from datetime import datetime
from typing import List, Dict, Optional
from collections import deque

logger = logging.getLogger(__name__)

class DataManager:
    """실시간 데이터 저장 및 관리 클래스"""
    
    def __init__(self, max_data_points: int = 100):
        self.max_data_points = max_data_points
        
        # 각 종목별 데이터 저장 (deque 사용으로 메모리 효율성)
        self.stock_data = {}  # {stock_code: {'prices': deque, 'volumes': deque, 'timestamps': deque}}
        
        # 거래 로그 파일 경로
        self.trade_log_file = f"trade_log_{datetime.now().strftime('%Y%m%d')}.csv"
        self._init_trade_log_file()
    
    def _init_trade_log_file(self):
        """거래 로그 CSV 파일 초기화"""
        if not os.path.exists(self.trade_log_file):
            with open(self.trade_log_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['시간', '종목코드', '매수/매도', '가격', '수량', '사유', '수익률'])
            logger.info(f"Created trade log file: {self.trade_log_file}")
    
    def add_tick_data(self, stock_code: str, price: float, volume: int, timestamp: str = None):
        """새로운 체결 데이터 추가"""
        if timestamp is None:
            timestamp = datetime.now().strftime('%H:%M:%S')
        
        if stock_code not in self.stock_data:
            self.stock_data[stock_code] = {
                'prices': deque(maxlen=self.max_data_points),
                'volumes': deque(maxlen=self.max_data_points),
                'timestamps': deque(maxlen=self.max_data_points),
                'highs': deque(maxlen=self.max_data_points),
                'lows': deque(maxlen=self.max_data_points)
            }
        
        data = self.stock_data[stock_code]
        data['prices'].append(price)
        data['volumes'].append(volume)
        data['timestamps'].append(timestamp)
        
        # 고가/저가 업데이트 (최근 데이터 기준)
        if len(data['prices']) >= 5:
            recent_prices = list(data['prices'])[-5:]
            data['highs'].append(max(recent_prices))
            data['lows'].append(min(recent_prices))
        else:
            data['highs'].append(price)
            data['lows'].append(price)
        
        logger.debug(f"Added tick data for {stock_code}: {price}원, 거래량: {volume}")
    
    def get_recent_prices(self, stock_code: str, count: int = None) -> List[float]:
        """최근 가격 데이터 반환"""
        if stock_code not in self.stock_data:
            return []
        
        prices = list(self.stock_data[stock_code]['prices'])
        
        if count is None:
            return prices
        else:
            return prices[-count:] if len(prices) >= count else prices
    
    def get_recent_volumes(self, stock_code: str, count: int = None) -> List[int]:
        """최근 거래량 데이터 반환"""
        if stock_code not in self.stock_data:
            return []
        
        volumes = list(self.stock_data[stock_code]['volumes'])
        
        if count is None:
            return volumes
        else:
            return volumes[-count:] if len(volumes) >= count else volumes
    
    def get_recent_highs_lows(self, stock_code: str, count: int = 20) -> Dict[str, List[float]]:
        """최근 고가/저가 데이터 반환"""
        if stock_code not in self.stock_data:
            return {'highs': [], 'lows': []}
        
        data = self.stock_data[stock_code]
        highs = list(data['highs'])[-count:] if len(data['highs']) >= count else list(data['highs'])
        lows = list(data['lows'])[-count:] if len(data['lows']) >= count else list(data['lows'])
        
        return {'highs': highs, 'lows': lows}
    
    def get_average_volume(self, stock_code: str, period: int = 20) -> float:
        """평균 거래량 계산"""
        volumes = self.get_recent_volumes(stock_code, period)
        if not volumes:
            return 0.0
        
        return sum(volumes) / len(volumes)
    
    def get_current_price(self, stock_code: str) -> Optional[float]:
        """현재가 반환"""
        if stock_code not in self.stock_data or not self.stock_data[stock_code]['prices']:
            return None
        
        return self.stock_data[stock_code]['prices'][-1]
    
    def save_trade_log(self, stock_code: str, action: str, price: float, quantity: int, reason: str, profit_rate: float = 0.0):
        """거래 내역을 CSV 파일에 저장"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            with open(self.trade_log_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([timestamp, stock_code, action, price, quantity, reason, f"{profit_rate:.2f}%"])
            
            logger.info(f"Trade log saved: {action} {stock_code} {quantity}주 @{price}원 ({reason})")
            
        except Exception as e:
            logger.error(f"Failed to save trade log: {e}")
    
    def get_data_count(self, stock_code: str) -> int:
        """저장된 데이터 개수 반환"""
        if stock_code not in self.stock_data:
            return 0
        
        return len(self.stock_data[stock_code]['prices'])
    
    def has_sufficient_data(self, stock_code: str, min_count: int = 20) -> bool:
        """충분한 데이터가 있는지 확인"""
        return self.get_data_count(stock_code) >= min_count
    
    def clear_old_data(self):
        """오래된 데이터 정리 (메모리 관리)"""
        for stock_code in self.stock_data:
            data = self.stock_data[stock_code]
            
            # deque는 자동으로 maxlen에 따라 관리되므로 추가 정리 불필요
            logger.debug(f"Current data count for {stock_code}: {len(data['prices'])}")
    
    def get_stock_summary(self, stock_code: str) -> Dict:
        """종목 데이터 요약 정보"""
        if stock_code not in self.stock_data:
            return {}
        
        prices = list(self.stock_data[stock_code]['prices'])
        volumes = list(self.stock_data[stock_code]['volumes'])
        
        if not prices:
            return {}
        
        return {
            'current_price': prices[-1],
            'data_count': len(prices),
            'price_range': {'high': max(prices), 'low': min(prices)},
            'avg_volume': sum(volumes) / len(volumes) if volumes else 0,
            'latest_volume': volumes[-1] if volumes else 0
        }