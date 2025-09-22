"""
재무비율 분석기
PBR, PER, ROE, PSR 등 밸류에이션 지표 계산 및 분석
"""

import logging
import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class ValuationMetrics:
    """밸류에이션 지표 데이터"""
    stock_code: str
    stock_name: str
    pbr: Optional[float] = None      # 주가순자산비율
    per: Optional[float] = None      # 주가수익비율
    roe: Optional[float] = None      # 자기자본이익률
    psr: Optional[float] = None      # 주가매출액비율
    current_price: Optional[float] = None
    market_cap: Optional[float] = None
    calculated_at: datetime = None
    
    def __post_init__(self):
        if self.calculated_at is None:
            self.calculated_at = datetime.now()

class ValuationAnalyzer:
    """재무비율 분석기"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.metrics_cache = {}  # 재무지표 캐시
        self.cache_duration_minutes = 60  # 1시간 캐시
        
    async def get_valuation_metrics(self, stock_code: str, force_refresh: bool = False) -> Optional[ValuationMetrics]:
        """종목의 밸류에이션 지표 조회"""
        try:
            # 캐시 확인 (강제 새로고침이 아닌 경우)
            if not force_refresh and self._is_cache_valid(stock_code):
                logger.debug(f"Using cached valuation data for {stock_code}")
                return self.metrics_cache[stock_code]
            
            logger.info(f"📊 Fetching valuation metrics for {stock_code}")
            
            # 현재가 조회
            price_data = await self.api_client.get_current_price(stock_code)
            if not price_data or price_data.get('rt_cd') != '0':
                logger.warning(f"Failed to get current price for {stock_code}")
                return None
            
            output = price_data['output']
            current_price = float(output.get('stck_prpr', 0))
            stock_name = output.get('hts_kor_isnm', stock_code)
            
            if current_price <= 0:
                logger.warning(f"Invalid price for {stock_code}: {current_price}")
                return None
            
            # 밸류에이션 지표 계산
            metrics = ValuationMetrics(
                stock_code=stock_code,
                stock_name=stock_name,
                current_price=current_price
            )
            
            # PBR 계산 (캐싱 적용)
            metrics.pbr = await self.api_client.get_pbr_cached(stock_code)

            # PER 계산 (캐싱 적용)
            metrics.per = await self.api_client.get_per_cached(stock_code)

            # ROE 계산 (캐싱 적용)
            metrics.roe = await self.api_client.get_roe_cached(stock_code)

            # PSR 계산 (캐싱 적용)
            metrics.psr = await self.api_client.get_psr_cached(stock_code)
            
            # 시가총액 계산 (대략적)
            try:
                shares_outstanding = float(output.get('lstg_st_cnt', 0))  # 상장주식수
                if shares_outstanding > 0:
                    metrics.market_cap = current_price * shares_outstanding
            except:
                pass
            
            # 캐시에 저장
            self.metrics_cache[stock_code] = metrics
            
            pbr_str = f"{metrics.pbr:.2f}" if metrics.pbr is not None else "N/A"
            logger.debug(f"Valuation metrics for {stock_name}({stock_code}): "
                        f"PBR={pbr_str}, "
                        f"Price={current_price:,.0f}원")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting valuation metrics for {stock_code}: {e}")
            return None
    
    def _is_cache_valid(self, stock_code: str) -> bool:
        """캐시 유효성 검사"""
        if stock_code not in self.metrics_cache:
            return False
        
        cached_metrics = self.metrics_cache[stock_code]
        if not cached_metrics.calculated_at:
            return False
        
        time_diff = (datetime.now() - cached_metrics.calculated_at).total_seconds() / 60
        return time_diff < self.cache_duration_minutes
    
    async def filter_by_pbr(self, stock_codes: List[str], min_pbr: float = 0.1, 
                           max_pbr: float = 2.0, require_data: bool = True) -> List[str]:
        """PBR 기준으로 종목 필터링"""
        filtered_stocks = []
        
        logger.info(f"🔍 Filtering {len(stock_codes)} stocks by PBR "
                   f"(range: {min_pbr}-{max_pbr})")
        
        for stock_code in stock_codes:
            try:
                metrics = await self.get_valuation_metrics(stock_code, force_refresh=True)
                if not metrics or metrics.pbr is None:
                    if require_data:
                        logger.warning(f"🚫 제외: {stock_code} - PBR 데이터 없음 (필수 데이터)")
                        continue
                    else:
                        logger.debug(f"No PBR data for {stock_code}, skipping")
                        continue
                
                if min_pbr <= metrics.pbr <= max_pbr:
                    filtered_stocks.append(stock_code)
                    logger.debug(f"✅ {metrics.stock_name}({stock_code}) "
                               f"PBR: {metrics.pbr:.2f} - PASSED")
                else:
                    logger.debug(f"❌ {metrics.stock_name}({stock_code}) "
                               f"PBR: {metrics.pbr:.2f} - FILTERED OUT")
                
                # API 호출 제한을 위한 지연
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Error filtering {stock_code} by PBR: {e}")
                continue
        
        logger.info(f"📈 PBR filtering result: {len(filtered_stocks)}/{len(stock_codes)} stocks passed")
        return filtered_stocks
    
    async def filter_by_per(self, stock_codes: List[str], min_per: float = 3.0, 
                           max_per: float = 20.0, require_data: bool = True) -> List[str]:
        """PER 기준으로 종목 필터링"""
        filtered_stocks = []
        
        logger.info(f"🔍 Filtering {len(stock_codes)} stocks by PER "
                   f"(range: {min_per}-{max_per})")
        
        for stock_code in stock_codes:
            try:
                metrics = await self.get_valuation_metrics(stock_code, force_refresh=True)
                if not metrics or metrics.per is None:
                    if require_data:
                        logger.warning(f"🚫 제외: {stock_code} - PER 데이터 없음 (필수 데이터)")
                        continue
                    else:
                        logger.debug(f"No PER data for {stock_code}, skipping")
                        continue
                
                if min_per <= metrics.per <= max_per:
                    filtered_stocks.append(stock_code)
                    logger.debug(f"✅ {metrics.stock_name}({stock_code}) "
                               f"PER: {metrics.per:.2f} - PASSED")
                else:
                    logger.debug(f"❌ {metrics.stock_name}({stock_code}) "
                               f"PER: {metrics.per:.2f} - FILTERED OUT")
                
                # API 호출 제한을 위한 지연
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Error filtering {stock_code} by PER: {e}")
                continue
        
        logger.info(f"📈 PER filtering result: {len(filtered_stocks)}/{len(stock_codes)} stocks passed")
        return filtered_stocks
    
    async def filter_by_roe(self, stock_codes: List[str], min_roe: float = 5.0, require_data: bool = True) -> List[str]:
        """ROE 기준으로 종목 필터링 (ROE가 높을수록 좋음)"""
        filtered_stocks = []
        
        logger.info(f"🔍 Filtering {len(stock_codes)} stocks by ROE "
                   f"(minimum: {min_roe}%)")
        
        for stock_code in stock_codes:
            try:
                metrics = await self.get_valuation_metrics(stock_code, force_refresh=True)
                if not metrics or metrics.roe is None:
                    if require_data:
                        logger.warning(f"🚫 제외: {stock_code} - ROE 데이터 없음 (필수 데이터)")
                        continue
                    else:
                        logger.debug(f"No ROE data for {stock_code}, skipping")
                        continue
                
                if metrics.roe >= min_roe:
                    filtered_stocks.append(stock_code)
                    logger.debug(f"✅ {metrics.stock_name}({stock_code}) "
                               f"ROE: {metrics.roe:.2f}% - PASSED")
                else:
                    logger.debug(f"❌ {metrics.stock_name}({stock_code}) "
                               f"ROE: {metrics.roe:.2f}% - FILTERED OUT")
                
                # API 호출 제한을 위한 지연
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Error filtering {stock_code} by ROE: {e}")
                continue
        
        logger.info(f"📈 ROE filtering result: {len(filtered_stocks)}/{len(stock_codes)} stocks passed")
        return filtered_stocks
    
    async def filter_by_psr(self, stock_codes: List[str], max_psr: float = 3.0, require_data: bool = True) -> List[str]:
        """PSR 기준으로 종목 필터링 (PSR이 낮을수록 좋음)"""
        filtered_stocks = []
        
        logger.info(f"🔍 Filtering {len(stock_codes)} stocks by PSR "
                   f"(maximum: {max_psr})")
        
        for stock_code in stock_codes:
            try:
                metrics = await self.get_valuation_metrics(stock_code, force_refresh=True)
                if not metrics or metrics.psr is None:
                    if require_data:
                        logger.warning(f"🚫 제외: {stock_code} - PSR 데이터 없음 (필수 데이터)")
                        continue
                    else:
                        logger.debug(f"No PSR data for {stock_code}, skipping")
                        continue
                
                if metrics.psr <= max_psr:
                    filtered_stocks.append(stock_code)
                    logger.debug(f"✅ {metrics.stock_name}({stock_code}) "
                               f"PSR: {metrics.psr:.2f} - PASSED")
                else:
                    logger.debug(f"❌ {metrics.stock_name}({stock_code}) "
                               f"PSR: {metrics.psr:.2f} - FILTERED OUT")
                
                # API 호출 제한을 위한 지연
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Error filtering {stock_code} by PSR: {e}")
                continue
        
        logger.info(f"📈 PSR filtering result: {len(filtered_stocks)}/{len(stock_codes)} stocks passed")
        return filtered_stocks
    
    async def get_valuation_score(self, stock_code: str, config: Dict) -> float:
        """종목의 밸류에이션 점수 계산 (0-100)"""
        try:
            metrics = await self.get_valuation_metrics(stock_code)
            if not metrics:
                return 0.0
            
            score = 0.0
            total_weight = 0.0
            
            # PBR 점수 (낮을수록 좋음)
            if metrics.pbr is not None:
                pbr_score = self._calculate_pbr_score(metrics.pbr)
                score += pbr_score * 1.0  # PBR 가중치
                total_weight += 1.0
                logger.debug(f"PBR score for {stock_code}: {pbr_score:.1f} (PBR: {metrics.pbr:.2f})")
            
            # PER 점수 (적정 범위가 좋음)
            if metrics.per is not None:
                per_score = self._calculate_per_score(metrics.per)
                score += per_score * 0.8  # PER 가중치
                total_weight += 0.8
                logger.debug(f"PER score for {stock_code}: {per_score:.1f} (PER: {metrics.per:.2f})")
            
            # ROE 점수 (높을수록 좋음)
            if metrics.roe is not None:
                roe_score = self._calculate_roe_score(metrics.roe)
                score += roe_score * 0.6  # ROE 가중치
                total_weight += 0.6
                logger.debug(f"ROE score for {stock_code}: {roe_score:.1f} (ROE: {metrics.roe:.2f}%)")
            
            # PSR 점수 (낮을수록 좋음)
            if metrics.psr is not None:
                psr_score = self._calculate_psr_score(metrics.psr)
                score += psr_score * 0.4  # PSR 가중치
                total_weight += 0.4
                logger.debug(f"PSR score for {stock_code}: {psr_score:.1f} (PSR: {metrics.psr:.2f})")
            
            if total_weight > 0:
                final_score = score / total_weight
                logger.debug(f"Final valuation score for {stock_code}: {final_score:.1f}")
                return final_score
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating valuation score for {stock_code}: {e}")
            return 0.0
    
    def _calculate_pbr_score(self, pbr: float) -> float:
        """PBR 점수 계산 (0-100, 낮은 PBR이 높은 점수)"""
        if pbr <= 0:
            return 0.0
        
        # 최적 PBR 범위: 0.5-1.5
        if 0.5 <= pbr <= 1.0:
            return 100.0
        elif 1.0 < pbr <= 1.5:
            return 90.0 - (pbr - 1.0) * 20  # 1.0에서 1.5로 갈수록 90->80
        elif 0.3 <= pbr < 0.5:
            return 80.0 + (pbr - 0.3) * 100  # 0.3에서 0.5로 갈수록 80->100
        elif 1.5 < pbr <= 2.0:
            return 80.0 - (pbr - 1.5) * 80   # 1.5에서 2.0로 갈수록 80->40
        elif 2.0 < pbr <= 3.0:
            return 40.0 - (pbr - 2.0) * 30   # 2.0에서 3.0로 갈수록 40->10
        else:
            return 10.0  # 극단적인 값들
    
    def _calculate_per_score(self, per: float) -> float:
        """PER 점수 계산 (0-100, 적정 PER 범위가 높은 점수)"""
        if per <= 0:
            return 0.0  # 적자 기업
        
        # 최적 PER 범위: 8-15 (적정 가치 평가 범위)
        if 8.0 <= per <= 12.0:
            return 100.0  # 최고 점수
        elif 12.0 < per <= 15.0:
            return 90.0 - (per - 12.0) * 10  # 12에서 15로 갈수록 90->60
        elif 5.0 <= per < 8.0:
            return 70.0 + (per - 5.0) * 10   # 5에서 8로 갈수록 70->100
        elif 15.0 < per <= 20.0:
            return 60.0 - (per - 15.0) * 8   # 15에서 20으로 갈수록 60->20
        elif 3.0 <= per < 5.0:
            return 50.0 + (per - 3.0) * 10   # 3에서 5로 갈수록 50->70
        elif 20.0 < per <= 30.0:
            return 20.0 - (per - 20.0) * 1.5 # 20에서 30으로 갈수록 20->5
        elif per < 3.0:
            return 30.0  # 너무 낮은 PER (의심스러운 수익)
        else:
            return 5.0   # 30 이상의 높은 PER (고평가)
    
    def _calculate_roe_score(self, roe: float) -> float:
        """ROE 점수 계산 (0-100, 높은 ROE가 높은 점수)"""
        if roe < 0:
            return 0.0  # 적자 기업
        
        # ROE 점수 매핑 (높을수록 좋음)
        if roe >= 20.0:
            return 100.0  # 최고 점수 (20% 이상)
        elif 15.0 <= roe < 20.0:
            return 90.0 + (roe - 15.0) * 2  # 15-20%: 90->100점
        elif 12.0 <= roe < 15.0:
            return 80.0 + (roe - 12.0) * 3.33  # 12-15%: 80->90점
        elif 10.0 <= roe < 12.0:
            return 70.0 + (roe - 10.0) * 5  # 10-12%: 70->80점
        elif 8.0 <= roe < 10.0:
            return 60.0 + (roe - 8.0) * 5   # 8-10%: 60->70점
        elif 5.0 <= roe < 8.0:
            return 40.0 + (roe - 5.0) * 6.67 # 5-8%: 40->60점
        elif 3.0 <= roe < 5.0:
            return 20.0 + (roe - 3.0) * 10   # 3-5%: 20->40점
        elif 1.0 <= roe < 3.0:
            return 10.0 + (roe - 1.0) * 5    # 1-3%: 10->20점
        else:
            return 5.0   # 1% 미만 (매우 낮은 수익성)
    
    def _calculate_psr_score(self, psr: float) -> float:
        """PSR 점수 계산 (0-100, 낮은 PSR이 높은 점수)"""
        if psr <= 0:
            return 0.0  # 무효한 PSR
        
        # PSR 점수 매핑 (낮을수록 좋음)
        if psr <= 0.5:
            return 100.0  # 최고 점수 (매우 저평가)
        elif 0.5 < psr <= 1.0:
            return 90.0 + (1.0 - psr) * 20  # 0.5-1.0: 90->100점
        elif 1.0 < psr <= 1.5:
            return 80.0 + (1.5 - psr) * 20  # 1.0-1.5: 80->90점
        elif 1.5 < psr <= 2.0:
            return 70.0 + (2.0 - psr) * 20  # 1.5-2.0: 70->80점
        elif 2.0 < psr <= 3.0:
            return 50.0 + (3.0 - psr) * 20  # 2.0-3.0: 50->70점
        elif 3.0 < psr <= 4.0:
            return 30.0 + (4.0 - psr) * 20  # 3.0-4.0: 30->50점
        elif 4.0 < psr <= 5.0:
            return 10.0 + (5.0 - psr) * 20  # 4.0-5.0: 10->30점
        elif 5.0 < psr <= 10.0:
            return 10.0 - (psr - 5.0) * 1.6  # 5.0-10.0: 10->2점
        else:
            return 2.0   # 10 이상 (심각한 고평가)
    
    async def get_valuation_summary(self, stock_codes: List[str]) -> str:
        """밸류에이션 분석 요약"""
        if not stock_codes:
            return "분석할 종목이 없습니다."
        
        summary = "📊 밸류에이션 분석 요약\n"
        summary += "=" * 40 + "\n"
        
        valid_metrics = []
        
        for stock_code in stock_codes[:5]:  # 상위 5개만 표시
            metrics = await self.get_valuation_metrics(stock_code)
            if metrics:
                valid_metrics.append(metrics)
        
        if not valid_metrics:
            return summary + "유효한 밸류에이션 데이터가 없습니다."
        
        # 개별 종목 정보
        for i, metrics in enumerate(valid_metrics, 1):
            summary += f"{i}. {metrics.stock_name}({metrics.stock_code})\n"
            summary += f"   현재가: {metrics.current_price:,.0f}원\n"
            if metrics.pbr:
                summary += f"   PBR: {metrics.pbr:.2f}\n"
            if metrics.per:
                summary += f"   PER: {metrics.per:.2f}\n"
            if metrics.roe:
                summary += f"   ROE: {metrics.roe:.2f}%\n"
            if metrics.psr:
                summary += f"   PSR: {metrics.psr:.2f}\n"
            if metrics.market_cap:
                summary += f"   시가총액: {metrics.market_cap/1e8:.0f}억원\n"
            summary += "\n"
        
        # 평균 지표
        pbr_values = [m.pbr for m in valid_metrics if m.pbr is not None]
        per_values = [m.per for m in valid_metrics if m.per is not None]
        roe_values = [m.roe for m in valid_metrics if m.roe is not None]
        psr_values = [m.psr for m in valid_metrics if m.psr is not None]
        
        if pbr_values:
            avg_pbr = sum(pbr_values) / len(pbr_values)
            summary += f"평균 PBR: {avg_pbr:.2f}\n"
        
        if per_values:
            avg_per = sum(per_values) / len(per_values)
            summary += f"평균 PER: {avg_per:.2f}\n"
        
        if roe_values:
            avg_roe = sum(roe_values) / len(roe_values)
            summary += f"평균 ROE: {avg_roe:.2f}%\n"
        
        if psr_values:
            avg_psr = sum(psr_values) / len(psr_values)
            summary += f"평균 PSR: {avg_psr:.2f}\n"
        
        return summary
    
    def clear_cache(self):
        """캐시 초기화"""
        self.metrics_cache.clear()
        logger.info("Valuation metrics cache cleared")