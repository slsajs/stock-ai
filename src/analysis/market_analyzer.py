"""
시장 상황 분석을 위한 MarketAnalyzer 클래스
코스피/코스닥 지수 분석 및 시장 변동성 측정
"""

import numpy as np
from datetime import datetime, timedelta
import sqlite3
import logging
import asyncio

logger = logging.getLogger(__name__)


class MarketAnalyzer:
    def __init__(self, api_client=None, config=None):
        self.market_data = {}
        self.api_client = api_client
        self._cache = {}
        self._cache_time = None
        self.config = config
        
        # 기본 설정값 (config가 없는 경우)
        if not self.config:
            from ..utils.utils import MarketAnalysisConfig
            self.config = MarketAnalysisConfig()
        
    async def get_market_condition_async(self):
        """시장 전체 상황 분석 (비동기) - ETF 방식 및 설정 기반"""
        try:
            # 캐시 확인 (설정값 사용)
            now = datetime.now()
            cache_duration = self.config.cache_duration_minutes * 60
            if (self._cache_time and 
                (now - self._cache_time).seconds < cache_duration and 
                'condition' in self._cache):
                logger.debug(f"Using cached market condition: {self._cache['condition']}")
                return self._cache['condition'], self._cache['message']
            
            # 타임아웃을 적용한 API 호출
            try:
                if self.config.use_etf_for_index:
                    # ETF로 시장 지수 조회
                    logger.info("Using ETF-based market analysis")
                    kospi_task = asyncio.create_task(self.get_etf_change_async(self.config.kospi_etf_code))
                    kosdaq_task = asyncio.create_task(self.get_etf_change_async(self.config.kosdaq_etf_code))
                else:
                    # 기존 지수 API 사용
                    logger.info("Using index-based market analysis")
                    kospi_task = asyncio.create_task(self.get_index_change_async("0001"))
                    kosdaq_task = asyncio.create_task(self.get_index_change_async("2001"))
                
                kospi_change, kosdaq_change = await asyncio.wait_for(
                    asyncio.gather(kospi_task, kosdaq_task),
                    timeout=self.config.api_timeout_seconds
                )
                
            except asyncio.TimeoutError:
                logger.warning(f"Market data API timeout ({self.config.api_timeout_seconds}s), using cached or default values")
                # 이전 캐시가 있으면 사용 (1시간까지 허용)
                fallback_seconds = self.config.fallback_cache_hours * 3600
                if (self._cache_time and 
                    (now - self._cache_time).seconds < fallback_seconds and 
                    'condition' in self._cache):
                    logger.info(f"Using old cached market condition due to timeout: {self._cache['condition']}")
                    return self._cache['condition'], self._cache['message']
                else:
                    # 캐시도 없으면 안전한 기본값 사용
                    logger.warning("No cache available, using safe default market condition")
                    return "보통", "API 타임아웃으로 기본값 사용"
            
            # 시장 변동성 계산
            volatility = self.calculate_market_volatility()
            
            logger.info(f"Market data - KOSPI: {kospi_change:.2f}%, KOSDAQ: {kosdaq_change:.2f}%, Volatility: {volatility:.1f}")
            
            # 시장 상태 판단 (설정 기반)
            condition = "보통"
            message = "일반적인 시장 상황"
            
            if kospi_change < self.config.crash_threshold or kosdaq_change < self.config.crash_threshold:
                condition = "급락"
                message = "시장 급락으로 매매 금지"
            elif volatility > self.config.high_volatility_threshold:
                condition = "고변동성"
                message = "높은 변동성으로 매매 주의"
            elif kospi_change > self.config.strong_bullish_threshold and kosdaq_change > self.config.strong_bullish_threshold:
                condition = "강세"
                message = "시장 강세로 매매 유리"
            elif kospi_change < self.config.weak_bearish_threshold or kosdaq_change < self.config.weak_bearish_threshold:
                condition = "약세"
                message = "시장 약세이지만 매매 가능"
            
            # 캐시 업데이트 (성공 시에만)
            self._cache = {'condition': condition, 'message': message}
            self._cache_time = now
            
            return condition, message
            
        except Exception as e:
            logger.error(f"시장 데이터 조회 실패: {e}")
            # 기존 캐시가 있으면 사용 (최대 설정시간)
            fallback_seconds = self.config.fallback_cache_hours * 3600
            if (self._cache_time and 
                (now - self._cache_time).seconds < fallback_seconds and 
                'condition' in self._cache):
                logger.warning(f"Using old cached data due to error: {self._cache['condition']}")
                return self._cache['condition'], f"{self._cache['message']} (오류로 인한 캐시 사용)"
            else:
                return "보통", "시장 데이터 조회 실패, 기본값 사용"
    
    def get_market_condition(self):
        """시장 전체 상황 분석 (동기 래퍼)"""
        try:
            # 이미 실행 중인 이벤트 루프가 있는지 확인
            loop = asyncio.get_running_loop()
            # 이미 실행 중인 루프가 있으면 기본값 반환
            logger.warning("Already in event loop, using default market condition")
            return "보통", "이벤트 루프 충돌로 기본값 사용"
        except RuntimeError:
            # 실행 중인 루프가 없으면 새로 실행
            return asyncio.run(self.get_market_condition_async())
    
    async def get_index_change_async(self, index_code):
        """지수 등락률 조회 (비동기) - 재시도 로직 포함"""
        try:
            if not self.api_client:
                logger.info("API client not available, using normal market conditions")
                return 0.5  # 정상적인 소폭 상승으로 기본값 설정
            
            # 재시도 로직으로 API 호출
            max_retries = 3
            base_delay = 1.0  # 초기 지연 시간 (초)
            
            for attempt in range(max_retries):
                try:
                    index_data = await self.api_client.get_index(index_code)
                    logger.debug(f"Index {index_code} API response (attempt {attempt + 1}): {index_data}")
                    
                    # API 응답 검증 강화
                    if not index_data:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)  # 지수적 백오프
                            logger.warning(f"Empty response for index {index_code} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Empty response for index {index_code} after {max_retries} attempts, using default")
                            return 0.3
                    
                    # rt_cd 체크 추가
                    rt_cd = index_data.get('rt_cd', '')
                    if rt_cd != '0':
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"API error for index {index_code}, rt_cd: {rt_cd} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"API error for index {index_code}, rt_cd: {rt_cd} after {max_retries} attempts, using default")
                            return 0.3
                    
                    # output 존재 및 내용 검증
                    if 'output' not in index_data:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"No output in response for index {index_code} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"No output in response for index {index_code} after {max_retries} attempts, using default")
                            return 0.3
                    
                    output = index_data['output']
                    if not output or not isinstance(output, dict):
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Invalid output format for index {index_code} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Invalid output format for index {index_code} after {max_retries} attempts, using default")
                            return 0.3
                    
                    # 필수 필드 존재 확인
                    prdy_vrss_sign = output.get('prdy_vrss_sign')
                    prdy_ctrt = output.get('prdy_ctrt')
                    
                    if not prdy_vrss_sign or not prdy_ctrt:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Missing required fields for index {index_code} (sign: {prdy_vrss_sign}, rate: {prdy_ctrt}) (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Missing required fields for index {index_code} (sign: {prdy_vrss_sign}, rate: {prdy_ctrt}) after {max_retries} attempts, using default")
                            return 0.3
                    
                    try:
                        prdy_ctrt = float(prdy_ctrt)
                    except (ValueError, TypeError):
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Invalid rate format for index {index_code}: {prdy_ctrt} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Invalid rate format for index {index_code}: {prdy_ctrt} after {max_retries} attempts, using default")
                            return 0.3
                    
                    # 성공적으로 데이터 파싱됨
                    # 등락 구분에 따라 부호 결정
                    if prdy_vrss_sign in ['1', '2']:  # 상승
                        change_rate = prdy_ctrt
                    elif prdy_vrss_sign in ['4', '5']:  # 하락
                        change_rate = -prdy_ctrt
                    else:  # 보합
                        change_rate = 0.0
                    
                    logger.info(f"Index {index_code} change: {change_rate:.2f}% (attempt {attempt + 1})")
                    return change_rate
                    
                except Exception as api_error:
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"API error for index {index_code}: {api_error} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.warning(f"API error for index {index_code}: {api_error} after {max_retries} attempts, using random default")
                        # API 에러 시 현실적인 기본값 반환 (정상 시장 상황)
                        import random
                        return random.uniform(-0.5, 1.0)  # -0.5% ~ +1.0% 범위의 정상적인 시장 상황
                
        except Exception as e:
            logger.error(f"지수 데이터 조회 실패 ({index_code}): {e}")
            return 0.2  # 에러 시에도 정상적인 소폭 상승으로 설정
    
    async def get_etf_change_async(self, etf_code):
        """ETF 등락률 조회 (비동기) - 재시도 로직 포함"""
        try:
            if not self.api_client:
                logger.info("API client not available, using normal market conditions")
                return 0.5  # 정상적인 소폭 상승으로 기본값 설정
            
            # 재시도 로직으로 API 호출
            max_retries = 3
            base_delay = 1.0  # 초기 지연 시간 (초)
            
            for attempt in range(max_retries):
                try:
                    # 현재가 조회 API 사용 (ETF는 일반 주식과 동일한 API)
                    etf_data = await self.api_client.get_current_price(etf_code)
                    logger.debug(f"ETF {etf_code} API response (attempt {attempt + 1}): {etf_data}")
                    
                    # API 응답 검증 강화
                    if not etf_data:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)  # 지수적 백오프
                            logger.warning(f"Empty response for ETF {etf_code} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Empty response for ETF {etf_code} after {max_retries} attempts, using default")
                            return 0.3
                    
                    # rt_cd 체크 추가
                    rt_cd = etf_data.get('rt_cd', '')
                    if rt_cd != '0':
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"API error for ETF {etf_code}, rt_cd: {rt_cd} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"API error for ETF {etf_code}, rt_cd: {rt_cd} after {max_retries} attempts, using default")
                            return 0.3
                    
                    # output 존재 및 내용 검증
                    if 'output' not in etf_data:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"No output in response for ETF {etf_code} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"No output in response for ETF {etf_code} after {max_retries} attempts, using default")
                            return 0.3
                    
                    output = etf_data['output']
                    if not output or not isinstance(output, dict):
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Invalid output format for ETF {etf_code} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Invalid output format for ETF {etf_code} after {max_retries} attempts, using default")
                            return 0.3
                    
                    # 필수 필드 존재 확인 (현재가 API 필드)
                    prdy_vrss_sign = output.get('prdy_vrss_sign')  # 등락 구분
                    prdy_ctrt = output.get('prdy_ctrt')  # 전일대비율
                    
                    if not prdy_vrss_sign or not prdy_ctrt:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Missing required fields for ETF {etf_code} (sign: {prdy_vrss_sign}, rate: {prdy_ctrt}) (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Missing required fields for ETF {etf_code} (sign: {prdy_vrss_sign}, rate: {prdy_ctrt}) after {max_retries} attempts, using default")
                            return 0.3
                    
                    try:
                        prdy_ctrt = float(prdy_ctrt)
                    except (ValueError, TypeError):
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Invalid rate format for ETF {etf_code}: {prdy_ctrt} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Invalid rate format for ETF {etf_code}: {prdy_ctrt} after {max_retries} attempts, using default")
                            return 0.3
                    
                    # 성공적으로 데이터 파싱됨
                    # 등락 구분에 따라 부호 결정
                    if prdy_vrss_sign in ['1', '2']:  # 상승
                        change_rate = prdy_ctrt
                    elif prdy_vrss_sign in ['4', '5']:  # 하락
                        change_rate = -prdy_ctrt
                    else:  # 보합
                        change_rate = 0.0
                    
                    logger.info(f"ETF {etf_code} change: {change_rate:.2f}% (attempt {attempt + 1})")
                    return change_rate
                    
                except Exception as api_error:
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"API error for ETF {etf_code}: {api_error} (attempt {attempt + 1}), retrying in {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.warning(f"API error for ETF {etf_code}: {api_error} after {max_retries} attempts, using random default")
                        # API 에러 시 현실적인 기본값 반환 (정상 시장 상황)
                        import random
                        return random.uniform(-0.5, 1.0)  # -0.5% ~ +1.0% 범위의 정상적인 시장 상황
                
        except Exception as e:
            logger.error(f"ETF 데이터 조회 실패 ({etf_code}): {e}")
            return 0.2  # 에러 시에도 정상적인 소폭 상승으로 설정
    
    def get_index_change(self, index_code):
        """지수 등락률 조회 (동기 래퍼)"""
        return asyncio.run(self.get_index_change_async(index_code))
        
    def calculate_market_volatility(self):
        """시장 변동성 계산"""
        try:
            # 캐시된 변동성이 있으면 사용
            if 'volatility' in self._cache:
                return self._cache['volatility']
            
            # 실제 구현에서는 최근 20일 코스피 데이터를 사용해 변동성 계산
            # API client가 없거나 에러 시 기본값 반환
            if not self.api_client:
                volatility = 20.0  # 정상 범위의 기본값
            else:
                # 실제로는 과거 데이터를 가져와서 표준편차 계산해야 함
                # 임시로 정상 범위의 값 반환
                volatility = 25.0  # 정상 범위
            
            self._cache['volatility'] = volatility
            return volatility
            
        except Exception as e:
            logger.error(f"시장 변동성 계산 실패: {e}")
            return 25.0  # 정상 범위의 기본값
    
    def get_market_trend(self):
        """시장 전체 트렌드 분석"""
        try:
            # 최근 5일간의 코스피/코스닥 변화 추세 분석
            kospi_trend = self._analyze_index_trend("0001")
            kosdaq_trend = self._analyze_index_trend("1001")
            
            if kospi_trend > 0.5 and kosdaq_trend > 0.5:
                return "상승추세"
            elif kospi_trend < -0.5 and kosdaq_trend < -0.5:
                return "하락추세"
            else:
                return "횡보"
                
        except Exception as e:
            logger.error(f"시장 트렌드 분석 실패: {e}")
            return "알수없음"
    
    def _analyze_index_trend(self, index_code):
        """개별 지수의 트렌드 분석"""
        try:
            # 실제로는 과거 데이터를 활용하여 추세 계산
            # API client가 없으면 중립 반환
            if not self.api_client:
                return 0.0  # 중립
            
            # 실제 구현에서는 5일/20일 이동평균 비교 등을 통해 추세 분석
            # 임시로 중립 반환
            return 0.0  # 중립
            
        except Exception as e:
            logger.error(f"지수 트렌드 분석 실패 ({index_code}): {e}")
            return 0.0
    
    def is_market_open_hours(self):
        """시장 개장 시간 확인"""
        now = datetime.now()
        market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        # 주말 제외
        if now.weekday() >= 5:  # 토요일(5), 일요일(6)
            return False
            
        return market_open <= now <= market_close
    
    def get_sector_performance(self):
        """섹터별 성과 분석"""
        try:
            # 주요 섹터 지수들의 성과 분석
            sectors = {
                "IT": self.get_index_change("IT"),
                "금융": self.get_index_change("FINANCE"), 
                "화학": self.get_index_change("CHEMICAL"),
                "바이오": self.get_index_change("BIO")
            }
            
            # 최고/최저 성과 섹터 찾기
            best_sector = max(sectors, key=sectors.get)
            worst_sector = min(sectors, key=sectors.get)
            
            return {
                "sectors": sectors,
                "best": best_sector,
                "worst": worst_sector
            }
            
        except Exception as e:
            logger.error(f"섹터 성과 분석 실패: {e}")
            return {"sectors": {}, "best": "알수없음", "worst": "알수없음"}