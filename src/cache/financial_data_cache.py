import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass
import threading

logger = logging.getLogger(__name__)

@dataclass
class CachedFinancialData:
    """금융 데이터 캐시 항목"""
    stock_code: str
    per: Optional[float] = None
    roe: Optional[float] = None
    psr: Optional[float] = None
    pbr: Optional[float] = None
    cached_at: datetime = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'stock_code': self.stock_code,
            'per': self.per,
            'roe': self.roe,
            'psr': self.psr,
            'pbr': self.pbr,
            'cached_at': self.cached_at.isoformat() if self.cached_at else None
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CachedFinancialData':
        cached_at = None
        if data.get('cached_at'):
            cached_at = datetime.fromisoformat(data['cached_at'])

        return cls(
            stock_code=data['stock_code'],
            per=data.get('per'),
            roe=data.get('roe'),
            psr=data.get('psr'),
            pbr=data.get('pbr'),
            cached_at=cached_at
        )

class FinancialDataCache:
    """금융 데이터 캐시 관리자"""

    def __init__(self, db_path: str = "data/financial_cache.db", cache_hours: int = 24):
        self.db_path = db_path
        self.cache_hours = cache_hours
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        """데이터베이스 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS financial_cache (
                        stock_code TEXT PRIMARY KEY,
                        per REAL,
                        roe REAL,
                        psr REAL,
                        pbr REAL,
                        cached_at TEXT NOT NULL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # 인덱스 생성
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cached_at
                    ON financial_cache(cached_at)
                """)

                conn.commit()
                logger.info(f"Financial cache database initialized: {self.db_path}")

        except Exception as e:
            logger.error(f"Failed to initialize cache database: {e}")
            raise

    def get_cached_data(self, stock_code: str) -> Optional[CachedFinancialData]:
        """캐시된 데이터 조회"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()

                    # 캐시 유효 시간 계산
                    cutoff_time = datetime.now() - timedelta(hours=self.cache_hours)

                    cursor.execute("""
                        SELECT * FROM financial_cache
                        WHERE stock_code = ? AND cached_at > ?
                    """, (stock_code, cutoff_time.isoformat()))

                    row = cursor.fetchone()
                    if row:
                        data = dict(row)
                        cached_data = CachedFinancialData.from_dict(data)
                        logger.debug(f"Cache hit for {stock_code}: {cached_data}")
                        return cached_data

                    logger.debug(f"Cache miss for {stock_code}")
                    return None

        except Exception as e:
            logger.error(f"Error getting cached data for {stock_code}: {e}")
            return None

    def set_cached_data(self, data: CachedFinancialData):
        """데이터 캐시 저장"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    if data.cached_at is None:
                        data.cached_at = datetime.now()

                    conn.execute("""
                        INSERT OR REPLACE INTO financial_cache
                        (stock_code, per, roe, psr, pbr, cached_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        data.stock_code,
                        data.per,
                        data.roe,
                        data.psr,
                        data.pbr,
                        data.cached_at.isoformat()
                    ))

                    conn.commit()
                    logger.debug(f"Cached data for {data.stock_code}: PER={data.per}, ROE={data.roe}, PSR={data.psr}")

        except Exception as e:
            logger.error(f"Error caching data for {data.stock_code}: {e}")

    def update_metric(self, stock_code: str, metric: str, value: Optional[float]):
        """특정 지표만 업데이트"""
        if metric not in ['per', 'roe', 'psr', 'pbr']:
            logger.warning(f"Invalid metric: {metric}")
            return

        try:
            # 기존 데이터 조회 (만료된 것도 포함)
            cached_data = self._get_any_cached_data(stock_code)

            if cached_data is None:
                # 새로운 데이터 생성
                cached_data = CachedFinancialData(stock_code=stock_code)

            # 지표 업데이트
            setattr(cached_data, metric, value)
            cached_data.cached_at = datetime.now()

            # 저장
            self.set_cached_data(cached_data)

        except Exception as e:
            logger.error(f"Error updating {metric} for {stock_code}: {e}")

    def _get_any_cached_data(self, stock_code: str) -> Optional[CachedFinancialData]:
        """만료 여부와 상관없이 캐시된 데이터 조회"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()

                    cursor.execute("""
                        SELECT * FROM financial_cache
                        WHERE stock_code = ?
                        ORDER BY cached_at DESC LIMIT 1
                    """, (stock_code,))

                    row = cursor.fetchone()
                    if row:
                        return CachedFinancialData.from_dict(dict(row))
                    return None

        except Exception as e:
            logger.error(f"Error getting any cached data for {stock_code}: {e}")
            return None

    def cleanup_expired(self):
        """만료된 캐시 데이터 정리"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    cutoff_time = datetime.now() - timedelta(hours=self.cache_hours * 2)  # 2배 기간 이후 삭제

                    cursor = conn.cursor()
                    cursor.execute("""
                        DELETE FROM financial_cache
                        WHERE cached_at < ?
                    """, (cutoff_time.isoformat(),))

                    deleted_count = cursor.rowcount
                    conn.commit()

                    if deleted_count > 0:
                        logger.info(f"Cleaned up {deleted_count} expired cache entries")

        except Exception as e:
            logger.error(f"Error cleaning up expired cache: {e}")

    def get_cache_stats(self) -> Dict[str, int]:
        """캐시 통계 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 총 캐시 항목 수
                cursor.execute("SELECT COUNT(*) FROM financial_cache")
                total_count = cursor.fetchone()[0]

                # 유효한 캐시 항목 수
                cutoff_time = datetime.now() - timedelta(hours=self.cache_hours)
                cursor.execute("""
                    SELECT COUNT(*) FROM financial_cache
                    WHERE cached_at > ?
                """, (cutoff_time.isoformat(),))
                valid_count = cursor.fetchone()[0]

                return {
                    'total': total_count,
                    'valid': valid_count,
                    'expired': total_count - valid_count
                }

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {'total': 0, 'valid': 0, 'expired': 0}