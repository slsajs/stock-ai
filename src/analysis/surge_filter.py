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
            
            # 급등주 판단
            surge_config = config.get('surge_filter', {})
            max_daily_change = surge_config.get('max_daily_change', 10.0)
            max_volume_ratio = surge_config.get('max_volume_ratio', 5.0)
            max_volatility = surge_config.get('max_volatility', 30.0)
            
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
        surge_config = config.get('surge_filter', {})
        max_surge_score = surge_config.get('max_surge_score', 70.0)
        
        self.logger.info(f"🚫 급등주 필터링 시작: {len(stock_codes)}개 종목")
        
        for stock_code in stock_codes:
            try:
                metrics = await self.analyze_surge_risk(stock_code, config)
                
                if metrics:
                    if not metrics.is_surge_stock and metrics.surge_score <= max_surge_score:
                        filtered_stocks.append(stock_code)
                        self.logger.info(f"✅ 통과: {metrics.stock_name}({stock_code}) - 급등점수 {metrics.surge_score:.1f}")
                    else:
                        self.logger.warning(f"🚫 제외: {metrics.stock_name}({stock_code}) - "
                                          f"급등위험 (점수: {metrics.surge_score:.1f}, "
                                          f"등락률: {metrics.daily_change_pct:.2f}%, "
                                          f"거래량: {metrics.volume_ratio:.1f}배)")
                else:
                    self.logger.warning(f"🚫 제외: {stock_code} - 데이터 조회 실패")
                    
            except Exception as e:
                self.logger.error(f"급등 필터링 오류 {stock_code}: {e}")
        
        self.logger.info(f"🎯 급등주 필터링 완료: {len(stock_codes)}개 → {len(filtered_stocks)}개")
        return filtered_stocks
    
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