import logging
import sqlite3
import asyncio
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import threading
import os
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("Pandas not available. Some features will be limited.")

class HybridDataManager:
    """
    실시간 매매 성능과 향후 AI 학습을 위한 대용량 데이터 저장을 모두 지원하는 데이터 관리 시스템
    - 메모리: 실시간 분석용 빠른 접근
    - SQLite: 장기 저장 및 AI 학습용 대용량 데이터
    """
    
    def __init__(self, symbol: str = "005930", max_memory_ticks: int = 1000, max_memory_minutes: int = 100, batch_size: int = 100):
        self.symbol = symbol
        self.batch_size = batch_size
        
        # 실시간 처리용: 메모리 (빠른 접근)
        self.recent_ticks = deque(maxlen=max_memory_ticks)  # 최근 1000개 체결 데이터
        self.recent_minutes = deque(maxlen=max_memory_minutes)  # 최근 100개 1분봉 데이터
        
        # 장기 저장용: SQLite DB
        self.db_path = f"stock_data_{symbol}.db"
        self._init_database()
        
        # 배치 저장용 버퍼
        self.batch_buffer = []
        self._lock = threading.Lock()  # 스레드 안전성을 위한 락
        
        # 1분봉 집계를 위한 임시 데이터
        self.current_minute_data = {}
        self.last_minute_timestamp = None
        
        logger.info(f"HybridDataManager initialized for {symbol} - DB: {self.db_path}")
    
    def _init_database(self):
        """데이터베이스 초기화 및 테이블 생성"""
        try:
            # 데이터베이스 디렉토리 생성
            db_dir = Path(self.db_path).parent
            db_dir.mkdir(exist_ok=True)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 체결 데이터 테이블 생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tick_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol VARCHAR(10) NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    volume INTEGER NOT NULL,
                    timestamp DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 분봉 데이터 테이블 생성 (집계용)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS minute_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol VARCHAR(10) NOT NULL,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume INTEGER,
                    minute_timestamp DATETIME UNIQUE,
                    rsi DECIMAL(5,2),
                    moving_avg_5 DECIMAL(10,2),
                    moving_avg_20 DECIMAL(10,2),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 인덱스 생성 (조회 성능 향상)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tick_timestamp ON tick_data(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tick_symbol ON tick_data(symbol)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_minute_timestamp ON minute_data(minute_timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_minute_symbol ON minute_data(symbol)")
            
            conn.commit()
            conn.close()
            
            logger.info(f"Database initialized successfully: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    def add_tick_data(self, price: float, volume: int, timestamp: datetime = None) -> bool:
        """
        새로운 체결 데이터 추가
        - 메모리에 즉시 저장 (실시간 분석용)
        - 배치 버퍼에 추가 (DB 저장 대기)
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        try:
            tick_data = {
                'price': float(price),
                'volume': int(volume),
                'timestamp': timestamp
            }
            
            with self._lock:
                # 1. 메모리에 즉시 저장 (실시간 매매 분석용)
                self.recent_ticks.append(tick_data)
                
                # 2. 배치 저장 버퍼에 추가
                self.batch_buffer.append(tick_data)
                
                # 3. 배치 크기에 도달하면 DB 저장
                if len(self.batch_buffer) >= self.batch_size:
                    self._save_batch_to_db()
                
                # 4. 1분봉 데이터 업데이트
                self._update_minute_data(price, volume, timestamp)
            
            logger.debug(f"Tick data added: {price}원, 거래량: {volume}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add tick data: {e}")
            return False
    
    def _save_batch_to_db(self):
        """배치로 DB에 저장 (성능 최적화)"""
        if not self.batch_buffer:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 배치 INSERT
            insert_data = [
                (self.symbol, tick['price'], tick['volume'], tick['timestamp']) 
                for tick in self.batch_buffer
            ]
            
            cursor.executemany(
                "INSERT INTO tick_data (symbol, price, volume, timestamp) VALUES (?, ?, ?, ?)",
                insert_data
            )
            
            conn.commit()
            conn.close()
            
            logger.info(f"DB에 {len(insert_data)}개 체결데이터 저장 완료")
            
            # 버퍼 초기화
            self.batch_buffer.clear()
            
        except Exception as e:
            logger.error(f"Batch save to DB failed: {e}")
    
    def _update_minute_data(self, price: float, volume: int, timestamp: datetime):
        """1분봉 데이터 업데이트"""
        try:
            # 분 단위로 자름 (초는 0으로)
            minute_key = timestamp.replace(second=0, microsecond=0)
            
            if minute_key not in self.current_minute_data:
                self.current_minute_data[minute_key] = {
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'count': 1
                }
            else:
                data = self.current_minute_data[minute_key]
                data['high'] = max(data['high'], price)
                data['low'] = min(data['low'], price)
                data['close'] = price
                data['volume'] += volume
                data['count'] += 1
            
            # 이전 분봉 데이터가 완료되면 메모리와 DB에 저장
            if self.last_minute_timestamp and minute_key > self.last_minute_timestamp:
                self._finalize_minute_data(self.last_minute_timestamp)
            
            self.last_minute_timestamp = minute_key
            
        except Exception as e:
            logger.error(f"Failed to update minute data: {e}")
    
    def _finalize_minute_data(self, minute_timestamp: datetime):
        """1분봉 데이터 완료 처리"""
        try:
            if minute_timestamp not in self.current_minute_data:
                return
            
            data = self.current_minute_data[minute_timestamp]
            
            # 기술적 지표 계산
            recent_prices = self.get_recent_prices(20)
            rsi = self._calculate_rsi(recent_prices) if len(recent_prices) >= 14 else None
            ma5 = sum(recent_prices[-5:]) / 5 if len(recent_prices) >= 5 else None
            ma20 = sum(recent_prices[-20:]) / 20 if len(recent_prices) >= 20 else None
            
            minute_data = {
                'timestamp': minute_timestamp,
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data['volume'],
                'rsi': rsi,
                'ma5': ma5,
                'ma20': ma20
            }
            
            # 메모리에 저장
            self.recent_minutes.append(minute_data)
            
            # DB에 저장
            self._save_minute_to_db(minute_data)
            
            # 완료된 데이터 제거
            del self.current_minute_data[minute_timestamp]
            
        except Exception as e:
            logger.error(f"Failed to finalize minute data: {e}")
    
    def _save_minute_to_db(self, minute_data: Dict):
        """1분봉 데이터를 DB에 저장"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO minute_data 
                (symbol, open_price, high_price, low_price, close_price, volume, 
                 minute_timestamp, rsi, moving_avg_5, moving_avg_20)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.symbol,
                minute_data['open'],
                minute_data['high'],
                minute_data['low'],
                minute_data['close'],
                minute_data['volume'],
                minute_data['timestamp'],
                minute_data['rsi'],
                minute_data['ma5'],
                minute_data['ma20']
            ))
            
            conn.commit()
            conn.close()
            
            logger.debug(f"Minute data saved to DB: {minute_data['timestamp']}")
            
        except Exception as e:
            logger.error(f"Failed to save minute data to DB: {e}")
    
    def get_recent_prices(self, count: int = 100) -> List[float]:
        """실시간 매매 분석용: 메모리에서 빠른 조회"""
        with self._lock:
            recent_data = list(self.recent_ticks)[-count:] if count else list(self.recent_ticks)
            return [tick['price'] for tick in recent_data]
    
    def get_recent_volumes(self, count: int = 100) -> List[int]:
        """최근 거래량 데이터 조회"""
        with self._lock:
            recent_data = list(self.recent_ticks)[-count:] if count else list(self.recent_ticks)
            return [tick['volume'] for tick in recent_data]
    
    def get_recent_minute_data(self, count: int = 20) -> List[Dict]:
        """최근 분봉 데이터 조회"""
        with self._lock:
            return list(self.recent_minutes)[-count:] if count else list(self.recent_minutes)
    
    def calculate_real_time_indicators(self) -> Optional[Dict]:
        """실시간 기술적 지표 계산"""
        try:
            prices = self.get_recent_prices(100)
            volumes = self.get_recent_volumes(100)
            
            if len(prices) < 14:
                return None
            
            indicators = {
                'rsi': self._calculate_rsi(prices),
                'ma5': sum(prices[-5:]) / 5 if len(prices) >= 5 else prices[-1],
                'ma20': sum(prices[-20:]) / 20 if len(prices) >= 20 else prices[-1],
                'volume_avg': sum(volumes[-20:]) / 20 if len(volumes) >= 20 else volumes[-1],
                'current_price': prices[-1] if prices else 0,
                'price_change': ((prices[-1] - prices[-2]) / prices[-2] * 100) if len(prices) >= 2 else 0,
                'data_count': len(prices)
            }
            
            return indicators
            
        except Exception as e:
            logger.error(f"Failed to calculate real-time indicators: {e}")
            return None
    
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        """RSI 지표 계산"""
        if len(prices) < period + 1:
            return None
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return None
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return round(rsi, 2)
    
    def force_save_batch(self):
        """강제로 배치 저장 (프로그램 종료 시 등)"""
        with self._lock:
            if self.batch_buffer:
                self._save_batch_to_db()
    
    def load_training_data(self, days: int = 30, include_indicators: bool = True) -> Optional['pd.DataFrame']:
        """AI 학습용: DB에서 대용량 데이터 로드"""
        if not PANDAS_AVAILABLE:
            logger.warning("Pandas not available. Cannot load training data as DataFrame.")
            return None
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            if include_indicators:
                # 기술적 지표가 포함된 분봉 데이터 조회
                query = """
                SELECT * FROM minute_data 
                WHERE symbol = ? AND minute_timestamp >= datetime('now', '-{} days')
                ORDER BY minute_timestamp ASC
                """.format(days)
                params = (self.symbol,)
            else:
                # 원시 체결 데이터 조회
                query = """
                SELECT * FROM tick_data 
                WHERE symbol = ? AND timestamp >= datetime('now', '-{} days')
                ORDER BY timestamp ASC
                """.format(days)
                params = (self.symbol,)
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            logger.info(f"{days}일간 데이터 {len(df)}건 로드 완료 (지표포함: {include_indicators})")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load training data: {e}")
            return None
    
    def export_data_for_ml(self, output_file: str = "training_data.csv", days: int = 90):
        """머신러닝용 CSV 파일로 내보내기"""
        try:
            df = self.load_training_data(days=days, include_indicators=True)
            if df is not None and not df.empty:
                df.to_csv(output_file, index=False, encoding='utf-8')
                logger.info(f"ML 학습용 데이터를 {output_file}로 저장 완료 ({len(df)}건)")
                return True
            else:
                logger.warning("No data to export")
                return False
                
        except Exception as e:
            logger.error(f"Failed to export data for ML: {e}")
            return False
    
    def get_data_statistics(self) -> Dict:
        """데이터 통계 정보 반환"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 체결 데이터 통계
            cursor.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM tick_data WHERE symbol = ?", 
                         (self.symbol,))
            tick_stats = cursor.fetchone()
            
            # 분봉 데이터 통계
            cursor.execute("SELECT COUNT(*), MIN(minute_timestamp), MAX(minute_timestamp) FROM minute_data WHERE symbol = ?", 
                         (self.symbol,))
            minute_stats = cursor.fetchone()
            
            conn.close()
            
            with self._lock:
                memory_ticks = len(self.recent_ticks)
                memory_minutes = len(self.recent_minutes)
                batch_pending = len(self.batch_buffer)
            
            return {
                'symbol': self.symbol,
                'db_tick_count': tick_stats[0] if tick_stats else 0,
                'db_tick_range': (tick_stats[1], tick_stats[2]) if tick_stats and tick_stats[1] else (None, None),
                'db_minute_count': minute_stats[0] if minute_stats else 0,
                'db_minute_range': (minute_stats[1], minute_stats[2]) if minute_stats and minute_stats[1] else (None, None),
                'memory_tick_count': memory_ticks,
                'memory_minute_count': memory_minutes,
                'batch_pending': batch_pending,
                'db_file_size': os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get data statistics: {e}")
            return {}
    
    def cleanup_old_data(self, keep_days: int = 30):
        """오래된 데이터 정리 (디스크 공간 관리)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=keep_days)
            
            # 체결 데이터 정리
            cursor.execute("DELETE FROM tick_data WHERE symbol = ? AND timestamp < ?", 
                         (self.symbol, cutoff_date))
            tick_deleted = cursor.rowcount
            
            # 분봉 데이터 정리
            cursor.execute("DELETE FROM minute_data WHERE symbol = ? AND minute_timestamp < ?", 
                         (self.symbol, cutoff_date))
            minute_deleted = cursor.rowcount
            
            # VACUUM으로 디스크 공간 정리
            cursor.execute("VACUUM")
            
            conn.commit()
            conn.close()
            
            logger.info(f"Old data cleanup completed: {tick_deleted} ticks, {minute_deleted} minutes deleted")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
    
    def __del__(self):
        """소멸자: 남은 배치 데이터 저장"""
        try:
            self.force_save_batch()
        except:
            pass