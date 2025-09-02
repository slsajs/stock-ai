import logging
import sqlite3
import asyncio
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import queue
import time
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
    - 비동기 DB 저장으로 데드락 방지
    """
    
    def __init__(self, symbol: str = "005930", max_memory_ticks: int = 1000, max_memory_minutes: int = 100, batch_size: int = 100):
        self.symbol = symbol
        self.batch_size = batch_size
        
        # 실시간 처리용: 메모리 (빠른 접근)
        self.recent_ticks = deque(maxlen=max_memory_ticks)
        self.recent_minutes = deque(maxlen=max_memory_minutes)
        
        # 장기 저장용: SQLite DB
        self.db_path = f"stock_data_{symbol}.db"
        self._init_database()
        
        # 스레드 안전성을 위한 락들 (분리)
        self._memory_lock = threading.RLock()  # 메모리 데이터용
        self._db_queue_lock = threading.RLock()  # DB 큐용
        
        # 비동기 DB 저장을 위한 큐와 워커
        self._db_queue = queue.Queue(maxsize=1000)  # 최대 1000개 대기
        self._db_worker_running = True
        self._db_worker_thread = threading.Thread(target=self._db_worker, daemon=True)
        self._db_worker_thread.start()
        
        # 1분봉 집계를 위한 임시 데이터
        self.current_minute_data = {}
        self.last_minute_timestamp = None
        
        # 성능 모니터링
        self._last_save_time = time.time()
        self._failed_saves = 0
        
        logger.info(f"HybridDataManager initialized for {symbol} - DB: {self.db_path}")
    
    def _init_database(self):
        """데이터베이스 초기화 및 테이블 생성"""
        try:
            db_dir = Path(self.db_path).parent
            db_dir.mkdir(exist_ok=True)
            
            # WAL 모드로 설정하여 동시 접근 개선
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")  # 성능 개선
            conn.execute("PRAGMA cache_size=10000")    # 캐시 크기 증가
            
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
            
            # 분봉 데이터 테이블 생성
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
            
            # 인덱스 생성
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
        새로운 체결 데이터 추가 - 논블로킹 방식
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        try:
            tick_data = {
                'price': float(price),
                'volume': int(volume),
                'timestamp': timestamp
            }
            
            # 1. 메모리에 즉시 저장 (빠른 락)
            with self._memory_lock:
                self.recent_ticks.append(tick_data)
                self._update_minute_data_safe(price, volume, timestamp)
            
            # 2. DB 저장 큐에 추가 (논블로킹)
            try:
                self._db_queue.put_nowait(('tick', tick_data))
            except queue.Full:
                logger.warning("DB queue is full, dropping oldest data")
                # 큐가 가득 찬 경우 가장 오래된 데이터 제거 후 재시도
                try:
                    self._db_queue.get_nowait()
                    self._db_queue.put_nowait(('tick', tick_data))
                except:
                    logger.error("Failed to add data to DB queue")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to add tick data: {e}")
            return False
    
    def _update_minute_data_safe(self, price: float, volume: int, timestamp: datetime):
        """1분봉 데이터 업데이트 - 메모리 락 내에서만 실행"""
        try:
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
            
            # 이전 분봉 완료 처리
            if self.last_minute_timestamp and minute_key > self.last_minute_timestamp:
                self._finalize_minute_data_safe(self.last_minute_timestamp)
            
            self.last_minute_timestamp = minute_key
            
        except Exception as e:
            logger.error(f"Failed to update minute data: {e}")
    
    def _finalize_minute_data_safe(self, minute_timestamp: datetime):
        """1분봉 데이터 완료 처리 - 메모리 락 내에서만 실행"""
        try:
            if minute_timestamp not in self.current_minute_data:
                return
            
            data = self.current_minute_data[minute_timestamp]
            
            # 기술적 지표 계산
            recent_prices = [tick['price'] for tick in list(self.recent_ticks)[-20:]]
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
            
            # DB 저장 큐에 추가
            try:
                self._db_queue.put_nowait(('minute', minute_data))
            except queue.Full:
                logger.warning("DB queue full, skipping minute data save")
            
            # 완료된 데이터 제거
            del self.current_minute_data[minute_timestamp]
            
        except Exception as e:
            logger.error(f"Failed to finalize minute data: {e}")
    
    def _db_worker(self):
        """DB 저장 전용 워커 스레드"""
        batch_buffer = []
        last_batch_time = time.time()
        
        logger.info("DB worker thread started")
        
        while self._db_worker_running:
            try:
                # 큐에서 데이터 가져오기 (타임아웃 설정)
                try:
                    data_type, data = self._db_queue.get(timeout=1.0)
                except queue.Empty:
                    # 타임아웃 시 배치 저장 확인
                    if batch_buffer and (time.time() - last_batch_time > 5.0):
                        self._save_batch_to_db(batch_buffer)
                        batch_buffer.clear()
                        last_batch_time = time.time()
                    continue
                
                if data_type == 'tick':
                    batch_buffer.append(data)
                    
                    # 배치 크기 도달 시 저장
                    if len(batch_buffer) >= self.batch_size:
                        self._save_batch_to_db(batch_buffer)
                        batch_buffer.clear()
                        last_batch_time = time.time()
                
                elif data_type == 'minute':
                    self._save_minute_to_db(data)
                
                elif data_type == 'shutdown':
                    # 종료 신호 처리
                    if batch_buffer:
                        self._save_batch_to_db(batch_buffer)
                    break
                
            except Exception as e:
                logger.error(f"DB worker error: {e}")
                self._failed_saves += 1
                time.sleep(0.1)  # 오류 시 잠시 대기
        
        logger.info("DB worker thread stopped")
    
    def _save_batch_to_db(self, batch_buffer: List[Dict]):
        """배치로 DB에 저장 - 워커 스레드에서만 실행"""
        if not batch_buffer:
            return
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            cursor = conn.cursor()
            
            # 배치 INSERT
            insert_data = [
                (self.symbol, tick['price'], tick['volume'], tick['timestamp']) 
                for tick in batch_buffer
            ]
            
            cursor.executemany(
                "INSERT INTO tick_data (symbol, price, volume, timestamp) VALUES (?, ?, ?, ?)",
                insert_data
            )
            
            conn.commit()
            self._last_save_time = time.time()
            logger.debug(f"DB에 {len(insert_data)}개 체결데이터 저장 완료")
            
        except Exception as e:
            logger.error(f"Batch save to DB failed: {e}")
            self._failed_saves += 1
        finally:
            if conn:
                conn.close()
    
    def _save_minute_to_db(self, minute_data: Dict):
        """1분봉 데이터를 DB에 저장 - 워커 스레드에서만 실행"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
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
            logger.debug(f"Minute data saved to DB: {minute_data['timestamp']}")
            
        except Exception as e:
            logger.error(f"Failed to save minute data to DB: {e}")
            self._failed_saves += 1
        finally:
            if conn:
                conn.close()
    
    def get_recent_prices(self, count: int = 100) -> List[float]:
        """실시간 매매 분석용: 메모리에서 빠른 조회"""
        with self._memory_lock:
            recent_data = list(self.recent_ticks)[-count:] if count else list(self.recent_ticks)
            return [tick['price'] for tick in recent_data]
    
    def get_recent_volumes(self, count: int = 100) -> List[int]:
        """최근 거래량 데이터 조회"""
        with self._memory_lock:
            recent_data = list(self.recent_ticks)[-count:] if count else list(self.recent_ticks)
            return [tick['volume'] for tick in recent_data]
    
    def get_recent_minute_data(self, count: int = 20) -> List[Dict]:
        """최근 분봉 데이터 조회"""
        with self._memory_lock:
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
        """강제로 배치 저장"""
        try:
            self._db_queue.put_nowait(('shutdown', None))
            # 새 워커 재시작
            if not self._db_worker_thread.is_alive():
                self._db_worker_running = True
                self._db_worker_thread = threading.Thread(target=self._db_worker, daemon=True)
                self._db_worker_thread.start()
        except Exception as e:
            logger.error(f"Failed to force save batch: {e}")
    
    def get_data_statistics(self) -> Dict:
        """데이터 통계 정보 반환"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM tick_data WHERE symbol = ?", 
                         (self.symbol,))
            tick_stats = cursor.fetchone()
            
            cursor.execute("SELECT COUNT(*), MIN(minute_timestamp), MAX(minute_timestamp) FROM minute_data WHERE symbol = ?", 
                         (self.symbol,))
            minute_stats = cursor.fetchone()
            
            conn.close()
            
            with self._memory_lock:
                memory_ticks = len(self.recent_ticks)
                memory_minutes = len(self.recent_minutes)
            
            queue_pending = self._db_queue.qsize()
            
            return {
                'symbol': self.symbol,
                'db_tick_count': tick_stats[0] if tick_stats else 0,
                'db_tick_range': (tick_stats[1], tick_stats[2]) if tick_stats and tick_stats[1] else (None, None),
                'db_minute_count': minute_stats[0] if minute_stats else 0,
                'db_minute_range': (minute_stats[1], minute_stats[2]) if minute_stats and minute_stats[1] else (None, None),
                'memory_tick_count': memory_ticks,
                'memory_minute_count': memory_minutes,
                'queue_pending': queue_pending,
                'failed_saves': self._failed_saves,
                'last_save_time': self._last_save_time,
                'worker_alive': self._db_worker_thread.is_alive(),
                'db_file_size': Path(self.db_path).stat().st_size if Path(self.db_path).exists() else 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get data statistics: {e}")
            return {'symbol': self.symbol, 'error': str(e)}
    
    def health_check(self) -> Dict[str, bool]:
        """시스템 상태 체크"""
        try:
            stats = self.get_data_statistics()
            
            return {
                'worker_alive': stats.get('worker_alive', False),
                'queue_healthy': stats.get('queue_pending', 0) < 500,  # 큐 크기 체크
                'db_accessible': 'error' not in stats,
                'recent_save': time.time() - stats.get('last_save_time', 0) < 60,  # 최근 1분 내 저장
                'low_failures': stats.get('failed_saves', 0) < 10
            }
        except:
            return {'worker_alive': False, 'queue_healthy': False, 'db_accessible': False, 'recent_save': False, 'low_failures': False}
    
    def load_training_data(self, days: int = 30, include_indicators: bool = True) -> Optional['pd.DataFrame']:
        """AI 학습용: DB에서 대용량 데이터 로드"""
        if not PANDAS_AVAILABLE:
            logger.warning("Pandas not available. Cannot load training data as DataFrame.")
            return None
        
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            
            if include_indicators:
                query = """
                SELECT * FROM minute_data 
                WHERE symbol = ? AND minute_timestamp >= datetime('now', '-{} days')
                ORDER BY minute_timestamp ASC
                """.format(days)
            else:
                query = """
                SELECT * FROM tick_data 
                WHERE symbol = ? AND timestamp >= datetime('now', '-{} days')
                ORDER BY timestamp ASC
                """.format(days)
            
            df = pd.read_sql_query(query, conn, params=(self.symbol,))
            conn.close()
            
            logger.info(f"{days}일간 데이터 {len(df)}건 로드 완료")
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
    
    def cleanup_old_data(self, keep_days: int = 30):
        """오래된 데이터 정리"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=keep_days)
            
            cursor.execute("DELETE FROM tick_data WHERE symbol = ? AND timestamp < ?", 
                         (self.symbol, cutoff_date))
            tick_deleted = cursor.rowcount
            
            cursor.execute("DELETE FROM minute_data WHERE symbol = ? AND minute_timestamp < ?", 
                         (self.symbol, cutoff_date))
            minute_deleted = cursor.rowcount
            
            cursor.execute("VACUUM")
            
            conn.commit()
            conn.close()
            
            logger.info(f"Old data cleanup: {tick_deleted} ticks, {minute_deleted} minutes deleted")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
    
    def shutdown(self):
        """안전한 종료"""
        logger.info(f"Shutting down HybridDataManager for {self.symbol}")
        
        try:
            # 워커 스레드 종료 신호
            self._db_worker_running = False
            self._db_queue.put_nowait(('shutdown', None))
            
            # 워커 스레드 종료 대기 (최대 5초)
            if self._db_worker_thread.is_alive():
                self._db_worker_thread.join(timeout=5.0)
                
            logger.info("HybridDataManager shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    def __del__(self):
        """소멸자"""
        try:
            self.shutdown()
        except:
            pass