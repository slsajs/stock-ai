#!/usr/bin/env python3
"""
급등주 필터링 시스템
급등한 종목들을 필터링하여 고점 매수를 방지
"""

import logging
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class SurgeMetrics:
    """급등 지표 데이터"""
    stock_code: str
    stock_name: str
    current_price: float
    daily_change_pct: float
    volume_ratio: float
    price_volatility: float
    is_surge_stock: bool
    surge_score: float
    
class SurgeFilter:
    """급등주 필터링 클래스"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
        
    async def analyze_surge_risk(self, stock_code: str, config: Dict[str, Any]) -> Optional[SurgeMetrics]:
        """급등 위험도 분석"""
        try:
            # 현재가 및 등락률 조회
            current_data = await self.api_client.get_current_price(stock_code)
            if not current_data:
                return None
                
            stock_name = current_data.get('output', {}).get('hts_kor_isnm', stock_code)
            current_price = float(current_data.get('output', {}).get('stck_prpr', 0))
            daily_change_pct = float(current_data.get('output', {}).get('prdy_ctrt', 0))
            
            # 거래량 비율 계산
            volume_ratio = await self._calculate_volume_ratio(stock_code)
            
            # 변동성 계산
            price_volatility = await self._calculate_price_volatility(stock_code)
            
            # 급등주 판단 기준 - 대폭 완화하여 거래 기회 확대
            surge_config = config.get('surge_filter', {})
            max_daily_change = surge_config.get('max_daily_change', 12.0)  # 7% → 12%로 대폭 완화
            max_volume_ratio = surge_config.get('max_volume_ratio', 15.0)  # 8배 → 15배로 대폭 완화
            max_volatility = surge_config.get('max_volatility', 30.0)  # 25 → 30으로 완화

            # 시간대별 완화 적용
            current_hour = datetime.now().hour
            if 9 <= current_hour <= 10:  # 장초반 완화
                max_daily_change *= 1.5  # 50% 완화
                max_volume_ratio *= 1.3  # 30% 완화
            elif 14 <= current_hour <= 15:  # 장후반 완화
                max_daily_change *= 1.2  # 20% 완화
            
            is_surge_stock = (
                abs(daily_change_pct) > max_daily_change or
                volume_ratio > max_volume_ratio or
                price_volatility > max_volatility
            )
            
            # 급등 점수 계산 (0-100, 높을수록 위험)
            surge_score = self._calculate_surge_score(
                daily_change_pct, volume_ratio, price_volatility
            )
            
            self.logger.info(f"🔍 급등 분석 {stock_name}({stock_code}): "
                           f"등락률 {daily_change_pct:.2f}%, 거래량비 {volume_ratio:.1f}배, "
                           f"변동성 {price_volatility:.1f}, 급등위험 {surge_score:.1f}/100")
            
            return SurgeMetrics(
                stock_code=stock_code,
                stock_name=stock_name,
                current_price=current_price,
                daily_change_pct=daily_change_pct,
                volume_ratio=volume_ratio,
                price_volatility=price_volatility,
                is_surge_stock=is_surge_stock,
                surge_score=surge_score
            )
            
        except Exception as e:
            self.logger.error(f"급등 분석 실패 {stock_code}: {e}")
            return None
    
    async def filter_surge_stocks(self, stock_codes: List[str], config: Dict[str, Any]) -> List[str]:
        """급등주 필터링"""
        if not config.get('surge_filter', {}).get('enable_surge_filter', False):
            return stock_codes

        filtered_stocks = []
        all_metrics = []  # 모든 종목의 급등 분석 결과 저장
        surge_config = config.get('surge_filter', {})
        max_surge_score = surge_config.get('max_surge_score', 85.0)  # 70 → 85로 완화

        self.logger.info(f"🚫 급등주 필터링 시작: {len(stock_codes)}개 종목")

        for stock_code in stock_codes:
            try:
                metrics = await self.analyze_surge_risk(stock_code, config)

                if metrics:
                    all_metrics.append(metrics)  # 모든 분석 결과 저장

                    # 기본 필터링 - 대폭 완화된 기준 적용
                    basic_pass = not metrics.is_surge_stock and metrics.surge_score <= max_surge_score

                    # 선별적 급등주 허용 로직 - 강한 상승 모멘텀이 있는 경우 예외 적용
                    momentum_exception = await self._check_momentum_exception(stock_code, metrics, config)

                    if basic_pass or momentum_exception:
                        filtered_stocks.append(stock_code)
                        reason = "기본통과" if basic_pass else "모멘텀예외"
                        self.logger.info(f"✅ 통과: {metrics.stock_name}({stock_code}) - {reason} (급등점수 {metrics.surge_score:.1f})")
                    else:
                        self.logger.warning(f"🚫 제외: {metrics.stock_name}({stock_code}) - "
                                          f"급등위험 (점수: {metrics.surge_score:.1f}, "
                                          f"등락률: {metrics.daily_change_pct:.2f}%, "
                                          f"거래량: {metrics.volume_ratio:.1f}배)")
                else:
                    self.logger.warning(f"🚫 제외: {stock_code} - 데이터 조회 실패")

            except Exception as e:
                self.logger.error(f"급등 필터링 오류 {stock_code}: {e}")

        # 필터링된 종목이 너무 적으면 대안 종목 추가
        if len(filtered_stocks) < max(1, len(stock_codes) * 0.3):  # 30% 미만이면
            try:
                alternative_stocks = await self._find_alternative_stocks(all_metrics, config)
                filtered_stocks.extend(alternative_stocks)
                self.logger.info(f"🔄 대안 종목 {len(alternative_stocks)}개 추가")
            except Exception as e:
                self.logger.error(f"대안 종목 찾기 실패: {e}")

        self.logger.info(f"🎯 급등주 필터링 완료: {len(stock_codes)}개 → {len(filtered_stocks)}개")
        return filtered_stocks

    async def _find_alternative_stocks(self, filtered_results: List[SurgeMetrics], config: Dict) -> List[str]:
        """급등주 대신 거래할 대안 종목 발굴"""
        try:
            # 급등 점수가 낮은 순으로 정렬하여 상위 몇 개 선택
            sorted_stocks = sorted(filtered_results, key=lambda x: x.surge_score)

            alternative_stocks = []
            for surge_metric in sorted_stocks:
                # 급등 점수가 40 미만이고, 거래량이 적당한 종목을 대안으로 선택
                if (surge_metric.surge_score < 40 and
                    surge_metric.volume_ratio >= 1.5 and  # 최소한의 거래량은 필요
                    abs(surge_metric.daily_change_pct) <= 5.0):  # 적당한 변동

                    alternative_stocks.append(surge_metric.stock_code)
                    self.logger.info(f"🎯 대안 종목 선정: {surge_metric.stock_name}({surge_metric.stock_code}) "
                                   f"점수:{surge_metric.surge_score:.1f}, 등락률:{surge_metric.daily_change_pct:.2f}%")

                    if len(alternative_stocks) >= 3:  # 최대 3개까지
                        break

            return alternative_stocks

        except Exception as e:
            self.logger.error(f"대안 종목 발굴 오류: {e}")
            return []

    async def _check_momentum_exception(self, stock_code: str, metrics: SurgeMetrics, config: Dict) -> bool:
        """강한 상승 모멘텀이 있는 급등주의 경우 거래 허용"""
        try:
            # 모멘텀 예외 조건들
            conditions = []

            # 1. 적당한 급등 + 강한 거래량 (건전한 급등)
            healthy_surge = (
                5.0 <= abs(metrics.daily_change_pct) <= 15.0 and  # 적당한 급등폭
                3.0 <= metrics.volume_ratio <= 20.0 and          # 건전한 거래량
                metrics.daily_change_pct > 0                     # 상승 중
            )
            conditions.append(healthy_surge)

            # 2. 소폭 상승 + 폭증 거래량 (관심종목)
            attention_stock = (
                0.5 <= metrics.daily_change_pct <= 8.0 and       # 소폭 상승
                metrics.volume_ratio >= 5.0                     # 거래량 폭증
            )
            conditions.append(attention_stock)

            # 3. 대형주는 더 관대하게 (삼성전자 등)
            if stock_code in ['005930', '000660', '035420', '005380', '068270']:  # 대형주
                large_cap_exception = (
                    abs(metrics.daily_change_pct) <= 20.0 and    # 20% 이내
                    metrics.volume_ratio >= 1.5                 # 거래량 증가
                )
                conditions.append(large_cap_exception)

            # 조건 중 하나라도 만족하면 예외 적용
            exception_granted = any(conditions)

            if exception_granted:
                self.logger.info(f"🎯 모멘텀 예외 적용: {metrics.stock_name}({stock_code}) - "
                               f"등락률: {metrics.daily_change_pct:.2f}%, 거래량: {metrics.volume_ratio:.1f}배")

            return exception_granted

        except Exception as e:
            self.logger.error(f"모멘텀 예외 검사 실패 {stock_code}: {e}")
            return False

    async def _calculate_volume_ratio(self, stock_code: str) -> float:
        """거래량 비율 계산 (당일 vs 평균)"""
        try:
            # 현재가 정보에서 거래량 추출
            current_data = await self.api_client.get_current_price(stock_code)
            if not current_data or current_data.get('rt_cd') != '0':
                return 1.0
                
            output = current_data.get('output', {})
            current_volume = float(output.get('acml_vol', 0))
            
            # 평균 거래량 대신 거래량 회전율 사용
            total_shares = float(output.get('lstg_st_cnt', 1))  # 상장주식수
            if total_shares > 0:
                volume_turnover = (current_volume / total_shares) * 100
                # 거래량 회전율이 1% 이상이면 높은 거래량으로 간주
                volume_ratio = max(1.0, volume_turnover * 10)  # 0.1% = 1배로 정규화
            else:
                volume_ratio = 1.0
            
            return min(volume_ratio, 10.0)  # 최대 10배로 제한
            
        except Exception as e:
            self.logger.error(f"거래량 비율 계산 실패 {stock_code}: {e}")
            return 1.0
    
    async def _calculate_price_volatility(self, stock_code: str) -> float:
        """가격 변동성 계산 (당일 등락률 기준)"""
        try:
            # 현재가 정보에서 등락률 추출
            current_data = await self.api_client.get_current_price(stock_code)
            if not current_data or current_data.get('rt_cd') != '0':
                return 0.0
                
            output = current_data.get('output', {})
            daily_change_pct = abs(float(output.get('prdy_ctrt', 0)))
            
            # 당일 등락률을 변동성 지표로 사용
            # 추가적으로 고가-저가 변동성도 계산
            try:
                high_price = float(output.get('stck_hgpr', 0))  # 고가
                low_price = float(output.get('stck_lwpr', 0))   # 저가
                current_price = float(output.get('stck_prpr', 0))  # 현재가
                
                if current_price > 0 and high_price > low_price:
                    intraday_volatility = ((high_price - low_price) / current_price) * 100
                    # 당일 등락률과 일중 변동성의 평균
                    volatility = (daily_change_pct + intraday_volatility) / 2
                else:
                    volatility = daily_change_pct
            except:
                volatility = daily_change_pct
            
            return volatility
            
        except Exception as e:
            self.logger.error(f"변동성 계산 실패 {stock_code}: {e}")
            return 0.0
    
    def _calculate_surge_score(self, daily_change_pct: float, volume_ratio: float, volatility: float) -> float:
        """급등 점수 계산 (0-100, 높을수록 위험)"""
        try:
            # 등락률 점수 (0-40점)
            change_score = min(abs(daily_change_pct) * 2, 40)
            
            # 거래량 점수 (0-35점)
            volume_score = min((volume_ratio - 1) * 7, 35)
            
            # 변동성 점수 (0-25점)
            volatility_score = min(volatility * 0.8, 25)
            
            total_score = change_score + volume_score + volatility_score
            return min(total_score, 100)
            
        except Exception:
            return 50.0  # 기본값