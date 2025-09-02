"""
시장 전체 동향 및 섹터별 분석기
KOSPI, KOSDAQ 지수 분석과 섹터 로테이션 분석
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class MarketCondition:
    """시장 상황 정보"""
    index_name: str
    current_value: float
    change_rate: float
    volume_ratio: float
    trend: str  # 상승/하락/보합
    strength: str  # 강세/약세/중립
    outlook: str  # 긍정/부정/중립

@dataclass
class SectorInfo:
    """섹터 정보"""
    sector_name: str
    performance: float  # 최근 성과
    momentum: float    # 모멘텀 점수
    volume_change: float  # 거래량 변화
    ranking: int       # 섹터 순위
    outlook: str       # 전망

class MarketSectorAnalyzer:
    """시장 및 섹터 분석기"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        
        # 주요 지수 코드
        self.major_indices = {
            "0001": "KOSPI",
            "1001": "KOSDAQ",
            "2001": "KOSPI200"
        }
        
        # 대표 섹터별 종목 (ETF나 대표 종목으로 섹터 분석)
        self.sector_stocks = {
            "IT/반도체": ["005930", "000660", "035420"],  # 삼성전자, SK하이닉스, 네이버
            "바이오": ["207940", "068270", "326030"],      # 삼성바이오로직스, 셀트리온, 백신테라퓨틱스
            "2차전지": ["373220", "066970", "051910"],     # LG에너지솔루션, 엘앤에프, LG화학
            "자동차": ["005380", "012330", "000270"],      # 현대차, 현대모비스, 기아
            "조선": ["009540", "010140", "067250"],        # HD한국조선해양, 삼성중공업, 현대위아
            "금융": ["055550", "086790", "316140"],        # 신한지주, 하나금융지주, 우리금융지주
            "화학": ["051910", "009150", "011170"],        # LG화학, 삼성전기, 롯데케미칼
            "건설": ["000720", "028050", "006360"],        # 현대건설, 삼성물산, GS건설
            "유통": ["023530", "069960", "282330"]         # 롯데쇼핑, 현대백화점, 현대홈쇼핑
        }
        
    async def analyze_market_condition(self) -> Dict[str, MarketCondition]:
        """전체 시장 상황 분석"""
        logger.info("📊 시장 전체 분석 시작")
        
        market_conditions = {}
        
        for i, (index_code, index_name) in enumerate(self.major_indices.items(), 1):
            try:
                condition = await self._analyze_single_index(index_code, index_name)
                if condition:
                    market_conditions[index_code] = condition
                    
                # API 호출 제한: 0.3초 대기
                await asyncio.sleep(0.3)
                logger.info(f"📊 지수 분석 완료 ({i}/{len(self.major_indices)}): {index_name}")
                
            except Exception as e:
                logger.error(f"지수 분석 오류 ({index_name}): {e}")
                continue
        
        # 전체 시장 상황 요약
        overall_condition = self._summarize_market_condition(market_conditions)
        logger.info(f"📈 시장 상황 요약: {overall_condition}")
        
        return market_conditions
    
    async def analyze_sector_rotation(self) -> List[SectorInfo]:
        """섹터 로테이션 분석"""
        logger.info("🔄 섹터 로테이션 분석 시작")
        
        sector_performances = []
        
        # 섹터 수를 제한하여 API 호출 감소
        limited_sectors = dict(list(self.sector_stocks.items())[:5])  # 상위 5개 섹터만
        
        for i, (sector_name, stock_codes) in enumerate(limited_sectors.items(), 1):
            try:
                logger.info(f"🔄 섹터 분석 중 ({i}/{len(limited_sectors)}): {sector_name}")
                performance = await self._calculate_sector_performance(sector_name, stock_codes[:2])  # 종목수도 2개로 제한
                if performance:
                    sector_performances.append(performance)
                    
                # API 호출 제한: 0.5초 대기
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"섹터 분석 오류 ({sector_name}): {e}")
                continue
        
        # 성과순 정렬
        sector_performances.sort(key=lambda x: x.performance, reverse=True)
        
        # 순위 매기기
        for i, sector in enumerate(sector_performances, 1):
            sector.ranking = i
        
        logger.info("📊 섹터별 성과 순위:")
        for sector in sector_performances[:5]:
            logger.info(f"  {sector.ranking}. {sector.sector_name}: {sector.performance:+.2f}% "
                      f"(모멘텀: {sector.momentum:.1f})")
        
        return sector_performances
    
    async def get_market_sentiment_score(self) -> float:
        """시장 심리 점수 계산 (0~100)"""
        try:
            market_conditions = await self.analyze_market_condition()
            sector_info = await self.analyze_sector_rotation()
            
            sentiment_score = 50  # 기본 중립값
            
            # 지수 상승률 기반 점수
            if "0001" in market_conditions:  # KOSPI
                kospi_change = market_conditions["0001"].change_rate
                if kospi_change > 1:
                    sentiment_score += 20
                elif kospi_change > 0:
                    sentiment_score += 10
                elif kospi_change < -1:
                    sentiment_score -= 20
                elif kospi_change < 0:
                    sentiment_score -= 10
            
            # 섹터 상승 비율
            if sector_info:
                positive_sectors = sum(1 for s in sector_info if s.performance > 0)
                sector_ratio = positive_sectors / len(sector_info)
                
                if sector_ratio > 0.7:
                    sentiment_score += 15
                elif sector_ratio > 0.5:
                    sentiment_score += 5
                elif sector_ratio < 0.3:
                    sentiment_score -= 15
                elif sector_ratio < 0.5:
                    sentiment_score -= 5
            
            return max(0, min(100, sentiment_score))
            
        except Exception as e:
            logger.error(f"시장 심리 점수 계산 오류: {e}")
            return 50
    
    async def _analyze_single_index(self, index_code: str, index_name: str) -> Optional[MarketCondition]:
        """개별 지수 분석"""
        try:
            # 현재 지수 정보 조회
            current_data = await self.api_client.get_index(index_code)
            if not current_data or current_data.get('rt_cd') != '0':
                return None
            
            output = current_data.get('output', {})
            current_value = float(output.get('bstp_nmix_prpr', 0))
            change_rate = float(output.get('bstp_nmix_prdy_ctrt', 0))
            
            # 과거 데이터로 추세 분석
            trend_analysis = await self._analyze_index_trend(index_code)
            
            # 거래량 분석
            volume_ratio = await self._calculate_index_volume_ratio(index_code)
            
            # 강도 및 전망 판단
            strength = self._determine_strength(change_rate, trend_analysis)
            trend = self._determine_trend(change_rate, trend_analysis)
            outlook = self._determine_outlook(change_rate, trend_analysis, volume_ratio)
            
            return MarketCondition(
                index_name=index_name,
                current_value=current_value,
                change_rate=change_rate,
                volume_ratio=volume_ratio,
                trend=trend,
                strength=strength,
                outlook=outlook
            )
            
        except Exception as e:
            logger.error(f"지수 분석 오류 ({index_name}): {e}")
            return None
    
    async def _analyze_index_trend(self, index_code: str) -> Dict[str, float]:
        """지수 추세 분석"""
        try:
            # 일봉 데이터 조회 (최근 30일)
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
            
            # 실제로는 지수 차트 API가 필요하지만, 여기서는 추정값 사용
            return {
                "short_term_trend": 0.5,  # 단기 추세 (0~1)
                "medium_term_trend": 0.6,  # 중기 추세
                "volatility": 0.4          # 변동성 (0~1)
            }
            
        except Exception as e:
            logger.error(f"지수 추세 분석 오류: {e}")
            return {"short_term_trend": 0.5, "medium_term_trend": 0.5, "volatility": 0.5}
    
    async def _calculate_index_volume_ratio(self, index_code: str) -> float:
        """지수 거래량 비율 계산"""
        try:
            # 실제로는 시장 전체 거래량 API가 필요
            # 여기서는 추정값 반환
            return 1.2  # 평균 대비 1.2배
        except:
            return 1.0
    
    async def _calculate_sector_performance(self, sector_name: str, stock_codes: List[str]) -> Optional[SectorInfo]:
        """섹터 성과 계산"""
        try:
            total_performance = 0
            total_momentum = 0
            total_volume_change = 0
            valid_stocks = 0
            
            for stock_code in stock_codes:
                try:
                    # 개별 주식 성과 분석
                    stock_performance = await self._analyze_stock_performance(stock_code)
                    if stock_performance:
                        total_performance += stock_performance['performance']
                        total_momentum += stock_performance['momentum']
                        total_volume_change += stock_performance['volume_change']
                        valid_stocks += 1
                        
                    await asyncio.sleep(0.3)
                    
                except Exception:
                    continue
            
            if valid_stocks == 0:
                return None
            
            # 평균 계산
            avg_performance = total_performance / valid_stocks
            avg_momentum = total_momentum / valid_stocks
            avg_volume_change = total_volume_change / valid_stocks
            
            # 전망 결정
            if avg_performance > 3 and avg_momentum > 60:
                outlook = "긍정"
            elif avg_performance < -3 or avg_momentum < 40:
                outlook = "부정"
            else:
                outlook = "중립"
            
            return SectorInfo(
                sector_name=sector_name,
                performance=avg_performance,
                momentum=avg_momentum,
                volume_change=avg_volume_change,
                ranking=0,  # 나중에 설정
                outlook=outlook
            )
            
        except Exception as e:
            logger.error(f"섹터 성과 계산 오류 ({sector_name}): {e}")
            return None
    
    async def _analyze_stock_performance(self, stock_code: str) -> Optional[Dict[str, float]]:
        """개별 주식 성과 분석"""
        try:
            # 현재가 조회
            current_data = await self.api_client.get_current_price(stock_code)
            if not current_data or current_data.get('rt_cd') != '0':
                return None
            
            output = current_data.get('output', {})
            current_price = int(output.get('stck_prpr', 0))
            change_rate = float(output.get('prdy_ctrt', 0))
            volume = int(output.get('acml_vol', 0))
            
            # 과거 데이터로 모멘텀 계산 (간단 버전)
            momentum = 50 + (change_rate * 2)  # 간단한 모멘텀 계산
            momentum = max(0, min(100, momentum))
            
            # 거래량 변화율 (임의값, 실제로는 과거 평균과 비교 필요)
            volume_change = 1.0 + (change_rate / 100)
            
            return {
                'performance': change_rate,
                'momentum': momentum,
                'volume_change': volume_change
            }
            
        except Exception:
            return None
    
    def _determine_strength(self, change_rate: float, trend_analysis: Dict) -> str:
        """시장 강도 판단"""
        if change_rate > 1.5 and trend_analysis.get('short_term_trend', 0.5) > 0.6:
            return "강세"
        elif change_rate < -1.5 and trend_analysis.get('short_term_trend', 0.5) < 0.4:
            return "약세"
        else:
            return "중립"
    
    def _determine_trend(self, change_rate: float, trend_analysis: Dict) -> str:
        """추세 판단"""
        if change_rate > 0.5:
            return "상승"
        elif change_rate < -0.5:
            return "하락"
        else:
            return "보합"
    
    def _determine_outlook(self, change_rate: float, trend_analysis: Dict, volume_ratio: float) -> str:
        """전망 판단"""
        score = 0
        
        if change_rate > 0:
            score += 1
        if trend_analysis.get('short_term_trend', 0.5) > 0.6:
            score += 1
        if volume_ratio > 1.2:
            score += 1
        
        if score >= 2:
            return "긍정"
        elif score <= 0:
            return "부정"
        else:
            return "중립"
    
    def _summarize_market_condition(self, conditions: Dict[str, MarketCondition]) -> str:
        """시장 상황 종합 판단"""
        if not conditions:
            return "분석 불가"
        
        positive_count = sum(1 for c in conditions.values() if c.change_rate > 0)
        total_count = len(conditions)
        
        if positive_count >= total_count * 0.8:
            return "전반적 상승세"
        elif positive_count >= total_count * 0.6:
            return "약간 상승세"
        elif positive_count >= total_count * 0.4:
            return "혼조세"
        else:
            return "전반적 하락세"
    
    async def get_favorable_sectors(self, top_n: int = 3) -> List[str]:
        """유리한 섹터 추천"""
        try:
            sector_info = await self.analyze_sector_rotation()
            
            # 성과와 모멘텀을 종합하여 점수 계산
            for sector in sector_info:
                sector.combined_score = (sector.performance * 0.6) + (sector.momentum * 0.4)
            
            # 점수순 정렬
            sector_info.sort(key=lambda x: getattr(x, 'combined_score', 0), reverse=True)
            
            return [sector.sector_name for sector in sector_info[:top_n]]
            
        except Exception as e:
            logger.error(f"유리한 섹터 분석 오류: {e}")
            return ["IT/반도체", "바이오", "2차전지"]  # 기본값