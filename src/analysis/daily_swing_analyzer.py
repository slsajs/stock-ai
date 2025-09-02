"""
일단위 단타를 위한 종합적인 주식 분석기
기술적 지표, 시장 상황, 거래량 분석을 통해 상승 가능성 높은 주식 선별
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import math

logger = logging.getLogger(__name__)

class DailySwingAnalyzer:
    """일단위 단타를 위한 종합 분석기"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        
        # 분석 가중치 설정
        self.weights = {
            'technical': 0.35,      # 기술적 지표 35%
            'volume': 0.25,         # 거래량 분석 25%
            'momentum': 0.20,       # 모멘텀 분석 20%
            'market_sentiment': 0.10,  # 시장 분위기 10%
            'price_pattern': 0.10   # 가격 패턴 10%
        }
        
        # 필터링 조건
        self.filters = {
            'min_price': 5000,      # 최소 주가
            'max_price': 200000,    # 최대 주가
            'min_volume': 100000,   # 최소 거래량
            'min_market_cap': 1000, # 최소 시가총액 (억원)
            'max_volatility': 0.15, # 최대 변동성 (15%)
            'exclude_sectors': ['금융업', '보험업']  # 제외 섹터
        }
        
    async def analyze_potential_winners(self, candidate_count: int = 100) -> List[Dict[str, Any]]:
        """상승 가능성 높은 주식들을 분석하여 순위별로 반환"""
        logger.info(f"🔍 일단위 단타 분석 시작 - 후보 {candidate_count}개 종목")
        
        try:
            # 1단계: 후보 종목 수집
            candidates = await self._get_candidate_stocks(candidate_count)
            logger.info(f"📋 후보 종목 {len(candidates)}개 수집 완료")
            
            # 2단계: 각 종목별 상세 분석 (API 제한 고려)
            analyzed_stocks = []
            max_analyze = min(10, len(candidates))  # 최대 10개만 분석 (API 제한)
            
            logger.info(f"📊 상세 분석할 종목 수: {max_analyze}개 (API 제한 고려)")
            
            for i, stock_info in enumerate(candidates[:max_analyze], 1):
                try:
                    stock_code = stock_info['stock_code']
                    logger.info(f"📊 분석 중 ({i}/{max_analyze}): {stock_info['stock_name']}({stock_code})")
                    
                    analysis_result = await self._analyze_single_stock(stock_code, stock_info)
                    if analysis_result:
                        analyzed_stocks.append(analysis_result)
                        
                    # API 호출 제한: 초당 2회로 제한 (0.5초 대기)
                    await asyncio.sleep(0.5)
                    
                    # 매 5개마다 추가 대기
                    if i % 5 == 0:
                        logger.info("🕒 API 제한으로 인한 추가 대기...")
                        await asyncio.sleep(2.0)  # 2초 대기
                    
                except Exception as e:
                    logger.error(f"종목 분석 오류 ({stock_code}): {e}")
                    continue
            
            # 3단계: 점수순 정렬 및 상위 종목 반환
            analyzed_stocks.sort(key=lambda x: x['total_score'], reverse=True)
            
            logger.info(f"✅ 분석 완료 - 상위 {min(10, len(analyzed_stocks))}개 종목:")
            for i, stock in enumerate(analyzed_stocks[:10], 1):
                logger.info(f"  {i}. {stock['stock_name']}({stock['stock_code']}): "
                          f"점수 {stock['total_score']:.1f} (상승확률: {stock['win_probability']:.1%})")
            
            return analyzed_stocks[:20]  # 상위 20개 반환
            
        except Exception as e:
            logger.error(f"주식 분석 중 오류: {e}")
            return []
    
    async def _get_candidate_stocks(self, count: int) -> List[Dict[str, Any]]:
        """후보 종목 수집 (거래량, 시가총액, 가격 기준 필터링)"""
        try:
            # 거래량 순위 조회
            volume_data = await self.api_client.get_volume_ranking(count=count)
            
            if not volume_data or volume_data.get('rt_cd') != '0':
                logger.warning("거래량 데이터 조회 실패, 기본 종목 사용")
                return [
                    {'stock_code': '005930', 'stock_name': '삼성전자'},
                    {'stock_code': '000660', 'stock_name': 'SK하이닉스'},
                    {'stock_code': '035420', 'stock_name': 'NAVER'}
                ]
            
            candidates = []
            for item in volume_data.get('output', [])[:count]:
                try:
                    stock_code = item.get('mksc_shrn_iscd', '').strip()
                    stock_name = item.get('hts_kor_isnm', '').strip()
                    current_price = int(item.get('stck_prpr', 0))
                    volume = int(item.get('acml_vol', 0))
                    
                    # 기본 필터링
                    if (stock_code and stock_name and 
                        self.filters['min_price'] <= current_price <= self.filters['max_price'] and
                        volume >= self.filters['min_volume']):
                        
                        candidates.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'volume': volume,
                            'change_rate': float(item.get('prdy_ctrt', 0))
                        })
                        
                except (ValueError, TypeError) as e:
                    continue
            
            return candidates
            
        except Exception as e:
            logger.error(f"후보 종목 수집 오류: {e}")
            return []
    
    async def _analyze_single_stock(self, stock_code: str, stock_info: Dict) -> Optional[Dict[str, Any]]:
        """개별 종목 상세 분석"""
        try:
            # 과거 데이터 조회 (30일)
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
            
            daily_data = await self.api_client.get_daily_price(stock_code, start_date, end_date)
            if not daily_data or daily_data.get('rt_cd') != '0':
                return None
                
            price_data = self._extract_price_data(daily_data)
            if len(price_data) < 20:  # 최소 20일 데이터 필요
                return None
            
            # 각 분석 영역별 점수 계산
            technical_score = self._calculate_technical_score(price_data)
            volume_score = self._calculate_volume_score(price_data, stock_info)
            momentum_score = self._calculate_momentum_score(price_data)
            pattern_score = self._calculate_price_pattern_score(price_data)
            
            # 시장 분위기 점수 (추후 구현)
            market_sentiment_score = 50  # 중립값
            
            # 가중 평균 계산
            total_score = (
                technical_score * self.weights['technical'] +
                volume_score * self.weights['volume'] +
                momentum_score * self.weights['momentum'] +
                pattern_score * self.weights['price_pattern'] +
                market_sentiment_score * self.weights['market_sentiment']
            )
            
            # 상승 확률 계산 (0~100점을 0~1 확률로 변환)
            win_probability = min(0.95, max(0.05, total_score / 100))
            
            return {
                'stock_code': stock_code,
                'stock_name': stock_info['stock_name'],
                'current_price': stock_info['current_price'],
                'total_score': total_score,
                'win_probability': win_probability,
                'technical_score': technical_score,
                'volume_score': volume_score,
                'momentum_score': momentum_score,
                'pattern_score': pattern_score,
                'market_sentiment_score': market_sentiment_score,
                'analysis_time': datetime.now(),
                'recommendation': self._get_recommendation(total_score),
                'expected_return': self._estimate_expected_return(total_score),
                'risk_level': self._assess_risk_level(price_data)
            }
            
        except Exception as e:
            logger.error(f"종목 분석 오류 ({stock_code}): {e}")
            return None
    
    def _extract_price_data(self, daily_data: Dict) -> List[Dict]:
        """일봉 데이터에서 필요한 정보 추출"""
        price_data = []
        
        for item in daily_data.get('output2', []):
            try:
                price_data.append({
                    'date': item.get('stck_bsop_date'),
                    'open': int(item.get('stck_oprc', 0)),
                    'high': int(item.get('stck_hgpr', 0)),
                    'low': int(item.get('stck_lwpr', 0)),
                    'close': int(item.get('stck_clpr', 0)),
                    'volume': int(item.get('acml_vol', 0)),
                    'change_rate': float(item.get('prdy_ctrt', 0))
                })
            except (ValueError, TypeError):
                continue
        
        # 날짜 순 정렬 (오래된 것부터)
        return sorted(price_data, key=lambda x: x['date'])
    
    def _calculate_technical_score(self, price_data: List[Dict]) -> float:
        """기술적 지표 기반 점수 계산 (0~100)"""
        if len(price_data) < 14:
            return 50  # 중립값
        
        closes = [d['close'] for d in price_data]
        volumes = [d['volume'] for d in price_data]
        
        scores = []
        
        # RSI 점수 (14일)
        rsi = self._calculate_rsi(closes, 14)
        if rsi:
            if 30 <= rsi <= 40:  # 과매도에서 회복 구간
                rsi_score = 80
            elif 40 <= rsi <= 60:  # 중립 구간
                rsi_score = 60
            elif rsi <= 30:  # 과매도
                rsi_score = 70  # 반등 기대
            else:  # 과매수
                rsi_score = 30
            scores.append(rsi_score)
        
        # 이동평균선 정렬 점수
        ma5 = sum(closes[-5:]) / 5 if len(closes) >= 5 else closes[-1]
        ma10 = sum(closes[-10:]) / 10 if len(closes) >= 10 else closes[-1]
        ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else closes[-1]
        
        current_price = closes[-1]
        
        ma_score = 0
        if current_price > ma5 > ma10 > ma20:  # 정배열
            ma_score = 90
        elif current_price > ma5 > ma10:
            ma_score = 70
        elif current_price > ma5:
            ma_score = 60
        else:
            ma_score = 40
        
        scores.append(ma_score)
        
        # MACD 점수 (간단 버전)
        if len(closes) >= 26:
            ema12 = self._calculate_ema(closes, 12)
            ema26 = self._calculate_ema(closes, 26)
            macd = ema12 - ema26
            
            if macd > 0:
                macd_score = 70
            else:
                macd_score = 40
            scores.append(macd_score)
        
        return sum(scores) / len(scores) if scores else 50
    
    def _calculate_volume_score(self, price_data: List[Dict], stock_info: Dict) -> float:
        """거래량 분석 점수 (0~100)"""
        if len(price_data) < 5:
            return 50
        
        volumes = [d['volume'] for d in price_data]
        recent_volume = volumes[-1]
        avg_volume = sum(volumes[-10:]) / min(10, len(volumes))
        
        # 거래량 급증 여부
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1
        
        if volume_ratio >= 2.0:  # 2배 이상 급증
            return 90
        elif volume_ratio >= 1.5:  # 1.5배 이상
            return 75
        elif volume_ratio >= 1.2:  # 1.2배 이상
            return 65
        elif volume_ratio >= 0.8:  # 정상 범위
            return 55
        else:  # 거래량 저조
            return 35
    
    def _calculate_momentum_score(self, price_data: List[Dict]) -> float:
        """모멘텀 분석 점수 (0~100)"""
        if len(price_data) < 5:
            return 50
        
        closes = [d['close'] for d in price_data]
        
        # 최근 5일 수익률
        recent_return = (closes[-1] / closes[-5] - 1) * 100
        
        # 최근 10일 vs 이전 10일 비교
        if len(closes) >= 20:
            recent_avg = sum(closes[-10:]) / 10
            prev_avg = sum(closes[-20:-10]) / 10
            trend_strength = (recent_avg / prev_avg - 1) * 100
        else:
            trend_strength = recent_return
        
        # 점수 계산
        momentum_score = 50  # 기본값
        
        if recent_return > 5:  # 5% 이상 상승
            momentum_score += 30
        elif recent_return > 2:  # 2% 이상 상승
            momentum_score += 20
        elif recent_return > 0:  # 소폭 상승
            momentum_score += 10
        elif recent_return > -2:  # 소폭 하락
            momentum_score -= 5
        else:  # 큰 폭 하락
            momentum_score -= 20
        
        if trend_strength > 3:
            momentum_score += 15
        elif trend_strength > 0:
            momentum_score += 5
        
        return max(0, min(100, momentum_score))
    
    def _calculate_price_pattern_score(self, price_data: List[Dict]) -> float:
        """가격 패턴 분석 점수 (0~100)"""
        if len(price_data) < 10:
            return 50
        
        closes = [d['close'] for d in price_data]
        highs = [d['high'] for d in price_data]
        lows = [d['low'] for d in price_data]
        
        score = 50
        
        # 지지선/저항선 돌파 여부
        current_price = closes[-1]
        recent_high = max(highs[-10:])  # 최근 10일 고점
        recent_low = min(lows[-10:])    # 최근 10일 저점
        
        # 고점 돌파 시 가점
        if current_price > recent_high * 0.98:  # 고점 근처 또는 돌파
            score += 20
        
        # 저점에서 반등 시 가점
        if current_price > recent_low * 1.05:  # 저점 대비 5% 이상 상승
            score += 15
        
        # 연속 상승일 체크
        consecutive_up = 0
        for i in range(len(closes) - 1, 0, -1):
            if closes[i] > closes[i-1]:
                consecutive_up += 1
            else:
                break
        
        if consecutive_up >= 3:
            score += 10
        elif consecutive_up >= 2:
            score += 5
        
        return max(0, min(100, score))
    
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        """RSI 계산"""
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
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """지수이동평균 계산"""
        if len(prices) < period:
            return sum(prices) / len(prices)
        
        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period
        
        for price in prices[period:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
        
        return ema
    
    def _get_recommendation(self, score: float) -> str:
        """점수 기반 추천 등급"""
        if score >= 80:
            return "강력 매수"
        elif score >= 70:
            return "매수"
        elif score >= 60:
            return "약한 매수"
        elif score >= 40:
            return "관망"
        else:
            return "매수 부적합"
    
    def _estimate_expected_return(self, score: float) -> float:
        """예상 수익률 추정 (%)"""
        # 점수를 기반으로 1-7일 예상 수익률
        return max(1.0, min(15.0, (score - 50) * 0.3))
    
    def _assess_risk_level(self, price_data: List[Dict]) -> str:
        """리스크 레벨 평가"""
        if len(price_data) < 10:
            return "중간"
        
        closes = [d['close'] for d in price_data]
        
        # 변동성 계산 (표준편차)
        returns = []
        for i in range(1, len(closes)):
            returns.append((closes[i] / closes[i-1] - 1) * 100)
        
        if returns:
            volatility = (sum([(r - sum(returns)/len(returns))**2 for r in returns]) / len(returns)) ** 0.5
            
            if volatility > 8:
                return "높음"
            elif volatility > 4:
                return "중간"
            else:
                return "낮음"
        
        return "중간"