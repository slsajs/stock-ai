"""
API 호출 제한 및 캐싱 관리
KIS API의 초당 호출 제한을 관리하고 결과를 캐싱
"""

import asyncio
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable
from functools import wraps

logger = logging.getLogger(__name__)

class APIThrottler:
    """API 호출 제한 관리자"""
    
    def __init__(self, max_calls_per_second: int = 2):  # 초당 2회로 더 강하게 제한
        self.max_calls_per_second = max_calls_per_second
        self.min_interval = 1.0 / max_calls_per_second
        self.last_call_time = 0
        self.call_count = 0
        self.start_time = time.time()
        
    async def throttle(self):
        """API 호출 제한 적용 (매우 보수적)"""
        current_time = time.time()
        
        # 무조건 최소 0.5초 대기 (초당 2회 보장)
        min_wait = 0.5
        
        # 마지막 호출 이후 시간 체크
        time_since_last = current_time - self.last_call_time
        if time_since_last < min_wait:
            wait_time = min_wait - time_since_last
            logger.info(f"🕒 API 안전을 위해 {wait_time:.2f}초 대기...")
            await asyncio.sleep(wait_time)
        
        # 1초 기준으로 호출 횟수 리셋
        if current_time - self.start_time >= 1.0:
            self.call_count = 0
            self.start_time = time.time()
        
        # 초당 최대 호출 횟수 체크 (더 보수적)
        if self.call_count >= self.max_calls_per_second:
            wait_time = 1.5  # 1.5초 대기
            logger.info(f"🚫 API 호출 한도 초과, {wait_time}초 대기...")
            await asyncio.sleep(wait_time)
            self.call_count = 0
            self.start_time = time.time()
        
        self.last_call_time = time.time()
        self.call_count += 1
        
        logger.debug(f"API 호출: {self.call_count}/{self.max_calls_per_second}")
        
    def reset(self):
        """통계 리셋"""
        self.call_count = 0
        self.start_time = time.time()
        self.last_call_time = 0

class APICache:
    """API 결과 캐싱"""
    
    def __init__(self, default_ttl: int = 60):  # 기본 1분 캐시
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.default_ttl = default_ttl
        
    def get(self, key: str) -> Optional[Any]:
        """캐시에서 값 조회"""
        if key in self.cache:
            cache_info = self.cache[key]
            if datetime.now() < cache_info['expires']:
                logger.debug(f"캐시 히트: {key}")
                return cache_info['data']
            else:
                # 만료된 캐시 삭제
                del self.cache[key]
                logger.debug(f"캐시 만료: {key}")
        
        return None
        
    def set(self, key: str, data: Any, ttl: Optional[int] = None):
        """캐시에 값 저장"""
        if ttl is None:
            ttl = self.default_ttl
            
        expires = datetime.now() + timedelta(seconds=ttl)
        self.cache[key] = {
            'data': data,
            'expires': expires
        }
        logger.debug(f"캐시 저장: {key} (TTL: {ttl}초)")
        
    def clear_expired(self):
        """만료된 캐시 정리"""
        current_time = datetime.now()
        expired_keys = [
            key for key, info in self.cache.items() 
            if current_time >= info['expires']
        ]
        
        for key in expired_keys:
            del self.cache[key]
            
        if expired_keys:
            logger.info(f"만료된 캐시 {len(expired_keys)}개 삭제")
    
    def get_cache_stats(self) -> Dict[str, int]:
        """캐시 통계"""
        return {
            'total_items': len(self.cache),
            'expired_items': sum(1 for info in self.cache.values() 
                               if datetime.now() >= info['expires'])
        }

# 전역 인스턴스
throttler = APIThrottler(max_calls_per_second=2)  # 초당 2회로 강하게 제한
cache = APICache(default_ttl=300)  # 5분 캐시

def throttled_api_call(cache_ttl: int = 300):
    """API 호출 제한 및 캐싱 데코레이터"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 캐시 키 생성
            cache_key = f"{func.__name__}:{str(args[1:]) + str(kwargs)}"
            
            # 캐시 확인
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # API 호출 제한 적용
            await throttler.throttle()
            
            try:
                # 실제 API 호출
                result = await func(*args, **kwargs)
                
                # 성공한 결과만 캐싱
                if result and isinstance(result, dict) and result.get('rt_cd') == '0':
                    cache.set(cache_key, result, cache_ttl)
                
                return result
                
            except Exception as e:
                logger.error(f"API 호출 오류 ({func.__name__}): {e}")
                raise
                
        return wrapper
    return decorator

# 주기적 캐시 정리 태스크
async def cache_cleanup_task():
    """캐시 정리 태스크 (백그라운드에서 실행)"""
    while True:
        try:
            await asyncio.sleep(300)  # 5분마다
            cache.clear_expired()
            
            stats = cache.get_cache_stats()
            logger.info(f"캐시 통계: {stats['total_items']}개 항목, "
                      f"{stats['expired_items']}개 만료")
                      
        except Exception as e:
            logger.error(f"캐시 정리 오류: {e}")

def get_api_stats() -> Dict[str, Any]:
    """API 사용 통계 조회"""
    return {
        'throttler': {
            'max_calls_per_second': throttler.max_calls_per_second,
            'current_calls': throttler.call_count,
            'last_call': throttler.last_call_time
        },
        'cache': cache.get_cache_stats()
    }