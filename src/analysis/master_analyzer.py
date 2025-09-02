"""
일단위 단타를 위한 마스터 분석기
모든 분석 도구들을 통합하여 최종 투자 추천 제공
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass

from .daily_swing_analyzer import DailySwingAnalyzer
from .market_sector_analyzer import MarketSectorAnalyzer

logger = logging.getLogger(__name__)

@dataclass
class InvestmentRecommendation:
    """투자 추천 정보"""
    stock_code: str
    stock_name: str
    recommendation: str  # 강력매수/매수/약한매수/관망
    confidence: float   # 신뢰도 (0~1)
    expected_return: float  # 예상 수익률 (%)
    risk_level: str     # 리스크 레벨
    hold_period: str    # 권장 보유기간
    entry_price: float  # 진입 가격
    target_price: float # 목표 가격
    stop_loss: float    # 손절 가격
    analysis_summary: str  # 분석 요약
    sector: str         # 소속 섹터

class MasterAnalyzer:
    """마스터 분석기 - 모든 분석을 통합"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.swing_analyzer = DailySwingAnalyzer(api_client)
        self.market_analyzer = MarketSectorAnalyzer(api_client)
        
    async def get_daily_recommendations(self, max_recommendations: int = 10) -> List[InvestmentRecommendation]:
        """일일 투자 추천 종목 생성"""
        logger.info(f"🎯 일일 투자 추천 분석 시작 (최대 {max_recommendations}개)")
        
        try:
            # 1. 시장 전체 상황 분석
            logger.info("📊 시장 분석 중...")
            market_conditions = await self.market_analyzer.analyze_market_condition()
            sector_analysis = await self.market_analyzer.analyze_sector_rotation()
            market_sentiment = await self.market_analyzer.get_market_sentiment_score()
            
            logger.info(f"💭 시장 심리 점수: {market_sentiment:.1f}/100")
            
            # 2. 유망 종목 분석
            logger.info("🔍 유망 종목 분석 중...")
            potential_stocks = await self.swing_analyzer.analyze_potential_winners(100)
            
            if not potential_stocks:
                logger.warning("분석된 종목이 없습니다.")
                return []
            
            # 3. 시장 상황을 고려한 필터링 및 조정
            filtered_stocks = await self._filter_by_market_condition(
                potential_stocks, market_conditions, sector_analysis, market_sentiment
            )
            
            # 4. 최종 추천 생성
            recommendations = []
            for stock_data in filtered_stocks[:max_recommendations]:
                recommendation = await self._create_recommendation(
                    stock_data, market_conditions, market_sentiment
                )
                if recommendation:
                    recommendations.append(recommendation)
            
            # 5. 신뢰도순 정렬
            recommendations.sort(key=lambda x: x.confidence, reverse=True)
            
            logger.info(f"✅ 최종 추천 종목 {len(recommendations)}개 생성")
            self._log_recommendations(recommendations[:5])
            
            return recommendations
            
        except Exception as e:
            logger.error(f"일일 추천 생성 오류: {e}")
            return []
    
    async def _filter_by_market_condition(self, stocks: List[Dict], market_conditions: Dict, 
                                        sector_analysis: List, market_sentiment: float) -> List[Dict]:
        """시장 상황을 고려한 종목 필터링"""
        
        # 유리한 섹터 파악
        favorable_sectors = await self.market_analyzer.get_favorable_sectors(5)
        logger.info(f"🔥 유리한 섹터: {', '.join(favorable_sectors)}")
        
        filtered_stocks = []
        
        for stock in stocks:
            # 기본 점수
            adjusted_score = stock['total_score']
            
            # 시장 심리에 따른 조정
            if market_sentiment > 70:  # 강세장
                if stock['momentum_score'] > 70:
                    adjusted_score += 10  # 모멘텀 종목 우대
            elif market_sentiment < 30:  # 약세장
                if stock['risk_level'] == '낮음':
                    adjusted_score += 5   # 안전 종목 우대
                else:
                    adjusted_score -= 10  # 리스크 종목 제외
            
            # 승률 기준 필터링 (시장 상황별)
            min_win_probability = self._get_min_win_probability(market_sentiment)
            if stock['win_probability'] < min_win_probability:
                continue
            
            # 조정된 점수로 업데이트
            stock['adjusted_score'] = adjusted_score
            filtered_stocks.append(stock)
        
        # 조정된 점수순 정렬
        filtered_stocks.sort(key=lambda x: x['adjusted_score'], reverse=True)
        
        return filtered_stocks
    
    def _get_min_win_probability(self, market_sentiment: float) -> float:
        """시장 상황별 최소 승률 기준"""
        if market_sentiment > 70:
            return 0.55  # 강세장: 55% 이상
        elif market_sentiment > 40:
            return 0.60  # 보통장: 60% 이상
        else:
            return 0.65  # 약세장: 65% 이상
    
    async def _create_recommendation(self, stock_data: Dict, market_conditions: Dict, 
                                   market_sentiment: float) -> Optional[InvestmentRecommendation]:
        """개별 종목 추천 정보 생성"""
        try:
            current_price = stock_data['current_price']
            expected_return = stock_data['expected_return']
            
            # 목표가와 손절가 계산
            target_price = current_price * (1 + expected_return / 100)
            
            # 시장 상황에 따른 손절선 조정
            if market_sentiment > 60:
                stop_loss_rate = -0.04  # 강세장: -4%
            elif market_sentiment > 40:
                stop_loss_rate = -0.035  # 보통: -3.5%
            else:
                stop_loss_rate = -0.03  # 약세장: -3% (타이트)
            
            stop_loss = current_price * (1 + stop_loss_rate)
            
            # 신뢰도 계산
            confidence = self._calculate_confidence(stock_data, market_sentiment)
            
            # 권장 보유기간
            hold_period = self._determine_hold_period(stock_data, market_sentiment)
            
            # 분석 요약 생성
            analysis_summary = self._generate_analysis_summary(stock_data, market_sentiment)
            
            # 섹터 추정 (실제로는 API에서 가져와야 함)
            sector = self._estimate_sector(stock_data['stock_name'])
            
            return InvestmentRecommendation(
                stock_code=stock_data['stock_code'],
                stock_name=stock_data['stock_name'],
                recommendation=stock_data['recommendation'],
                confidence=confidence,
                expected_return=expected_return,
                risk_level=stock_data['risk_level'],
                hold_period=hold_period,
                entry_price=current_price,
                target_price=target_price,
                stop_loss=stop_loss,
                analysis_summary=analysis_summary,
                sector=sector
            )
            
        except Exception as e:
            logger.error(f"추천 정보 생성 오류 ({stock_data.get('stock_code', 'Unknown')}): {e}")
            return None
    
    def _calculate_confidence(self, stock_data: Dict, market_sentiment: float) -> float:
        """신뢰도 계산 (0~1)"""
        base_confidence = stock_data['win_probability']
        
        # 시장 상황에 따른 조정
        if market_sentiment > 60:
            market_factor = 1.1  # 강세장에서 신뢰도 증가
        elif market_sentiment < 40:
            market_factor = 0.9  # 약세장에서 신뢰도 감소
        else:
            market_factor = 1.0
        
        # 기술적 점수에 따른 조정
        if stock_data['technical_score'] > 80:
            technical_factor = 1.1
        elif stock_data['technical_score'] < 50:
            technical_factor = 0.9
        else:
            technical_factor = 1.0
        
        confidence = base_confidence * market_factor * technical_factor
        return min(0.95, max(0.05, confidence))
    
    def _determine_hold_period(self, stock_data: Dict, market_sentiment: float) -> str:
        """권장 보유기간 결정"""
        if stock_data['momentum_score'] > 80 and market_sentiment > 60:
            return "1-3일"  # 강한 모멘텀 + 강세장
        elif stock_data['total_score'] > 75:
            return "3-7일"  # 높은 점수
        else:
            return "5-10일"  # 일반적인 경우
    
    def _generate_analysis_summary(self, stock_data: Dict, market_sentiment: float) -> str:
        """분석 요약 생성"""
        summary_parts = []
        
        # 기술적 분석 요약
        if stock_data['technical_score'] > 75:
            summary_parts.append("기술적 지표 강세")
        elif stock_data['technical_score'] < 50:
            summary_parts.append("기술적 지표 약세")
        
        # 거래량 분석 요약
        if stock_data['volume_score'] > 75:
            summary_parts.append("거래량 급증")
        
        # 모멘텀 분석 요약
        if stock_data['momentum_score'] > 75:
            summary_parts.append("상승 모멘텀 강함")
        
        # 시장 상황 고려
        if market_sentiment > 60:
            summary_parts.append("시장 분위기 양호")
        elif market_sentiment < 40:
            summary_parts.append("시장 분위기 부정적")
        
        return ", ".join(summary_parts) if summary_parts else "기본 분석 기준"
    
    def _estimate_sector(self, stock_name: str) -> str:
        """종목명으로 섹터 추정 (간단 버전)"""
        if any(keyword in stock_name for keyword in ['전자', '반도체', 'IT']):
            return "IT/반도체"
        elif any(keyword in stock_name for keyword in ['바이오', '제약', '헬스']):
            return "바이오"
        elif any(keyword in stock_name for keyword in ['전지', 'LG', '화학']):
            return "2차전지"
        elif any(keyword in stock_name for keyword in ['자동차', '현대차', '기아']):
            return "자동차"
        elif any(keyword in stock_name for keyword in ['조선', '해양', '중공업']):
            return "조선"
        elif any(keyword in stock_name for keyword in ['금융', '은행', '지주', '증권']):
            return "금융"
        elif any(keyword in stock_name for keyword in ['건설', '물산']):
            return "건설"
        else:
            return "기타"
    
    def _log_recommendations(self, recommendations: List[InvestmentRecommendation]) -> None:
        """추천 결과 로깅"""
        logger.info("🎯 상위 추천 종목:")
        for i, rec in enumerate(recommendations, 1):
            logger.info(f"  {i}. {rec.stock_name}({rec.stock_code}) - {rec.recommendation}")
            logger.info(f"     신뢰도: {rec.confidence:.1%}, 예상수익: {rec.expected_return:.1f}%, "
                      f"목표가: {rec.target_price:,.0f}원")
            logger.info(f"     분석: {rec.analysis_summary}")
    
    async def get_position_exit_recommendation(self, stock_code: str, entry_price: float, 
                                             current_price: float, days_held: int) -> Dict[str, Any]:
        """보유 포지션 매도 추천"""
        try:
            # 현재 수익률
            current_return = (current_price - entry_price) / entry_price * 100
            
            # 시장 상황 재분석
            market_sentiment = await self.market_analyzer.get_market_sentiment_score()
            
            # 개별 종목 재분석
            stock_info = {'stock_code': stock_code, 'stock_name': '', 'current_price': current_price}
            reanalysis = await self.swing_analyzer._analyze_single_stock(stock_code, stock_info)
            
            recommendation = "보유"  # 기본값
            confidence = 0.5
            reason = ""
            
            # 손익 기준 판단
            if current_return <= -3.0:  # -3% 이하
                recommendation = "손절"
                confidence = 0.9
                reason = f"손실 {current_return:.1f}% - 손절 기준 도달"
            elif current_return >= 7.0:  # +7% 이상
                recommendation = "익절"
                confidence = 0.8
                reason = f"수익 {current_return:.1f}% - 익절 고려"
            elif current_return >= 4.0:  # +4% 이상
                if reanalysis and reanalysis['total_score'] < 60:
                    recommendation = "부분익절"
                    confidence = 0.7
                    reason = f"수익 {current_return:.1f}% - 기술적 지표 약화"
            
            # 보유 기간 고려
            if days_held >= 10:  # 10일 이상 보유
                if current_return > 0:
                    recommendation = "익절"
                    confidence = min(0.9, confidence + 0.1)
                    reason += " (장기 보유)"
            
            # 시장 상황 고려
            if market_sentiment < 30:  # 약세장
                if current_return > 2:  # 2% 이상 수익 시 익절 권장
                    recommendation = "익절"
                    confidence = min(0.9, confidence + 0.2)
                    reason += " (시장 분위기 악화)"
            
            return {
                'recommendation': recommendation,
                'confidence': confidence,
                'reason': reason,
                'current_return': current_return,
                'market_sentiment': market_sentiment,
                'suggested_action': self._get_suggested_action(recommendation, current_return)
            }
            
        except Exception as e:
            logger.error(f"포지션 매도 추천 오류: {e}")
            return {
                'recommendation': '보유',
                'confidence': 0.5,
                'reason': '분석 오류',
                'current_return': current_return,
                'suggested_action': '상황 지켜보기'
            }
    
    def _get_suggested_action(self, recommendation: str, current_return: float) -> str:
        """구체적인 행동 제안"""
        if recommendation == "손절":
            return "즉시 매도"
        elif recommendation == "익절":
            return "전량 매도"
        elif recommendation == "부분익절":
            return "50% 매도 후 상황 관찰"
        else:
            if current_return > 3:
                return "트레일링 스톱으로 관리"
            else:
                return "목표가까지 보유"