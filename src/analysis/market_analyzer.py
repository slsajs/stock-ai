"""
시장 상황 분석을 위한 MarketAnalyzer 클래스
코스피/코스닥 지수 분석 및 시장 변동성 측정
"""

import numpy as np
from datetime import datetime, timedelta
import sqlite3
import logging

logger = logging.getLogger(__name__)


class MarketAnalyzer:
    def __init__(self):
        self.market_data = {}
        
    def get_market_condition(self):
        """시장 전체 상황 분석"""
        try:
            # KIS API로 코스피/코스닥 지수 조회
            kospi_change = self.get_index_change("0001")  # 코스피
            kosdaq_change = self.get_index_change("1001")  # 코스닥
            
            # 시장 변동성 계산
            volatility = self.calculate_market_volatility()
            
            # 시장 상태 판단
            if kospi_change < -1.5 or kosdaq_change < -1.5:
                return "급락", "시장 급락으로 매매 금지"
                
            if volatility > 30:  # 변동성 임계값
                return "고변동성", "높은 변동성으로 매매 주의"
                
            if kospi_change > 1.0 and kosdaq_change > 1.0:
                return "강세", "시장 강세로 매매 유리"
                
            return "보통", "일반적인 시장 상황"
            
        except Exception as e:
            logger.error(f"시장 데이터 조회 실패: {e}")
            return "데이터오류", f"시장 데이터 조회 실패: {e}"
    
    def get_index_change(self, index_code):
        """지수 등락률 조회"""
        try:
            # 임시로 랜덤값 반환 (실제로는 KIS API 호출)
            # 실제 구현시에는 기존 KIS API 함수를 활용해야 함
            import random
            return random.uniform(-3.0, 3.0)
        except Exception as e:
            logger.error(f"지수 데이터 조회 실패 ({index_code}): {e}")
            return 0.0
        
    def calculate_market_volatility(self):
        """시장 변동성 계산"""
        try:
            # 실제로는 최근 20일 코스피 데이터를 사용해 변동성을 계산
            # 임시로 랜덤값 반환
            import random
            return random.uniform(10.0, 40.0)
        except Exception as e:
            logger.error(f"시장 변동성 계산 실패: {e}")
            return 20.0  # 기본값
    
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
            # 임시로 랜덤값 반환
            import random
            return random.uniform(-1.0, 1.0)
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