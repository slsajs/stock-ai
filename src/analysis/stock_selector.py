import logging
import asyncio
from datetime import datetime, time
from typing import List, Dict, Optional
from ..api import KISAPIClient
from .valuation_analyzer import ValuationAnalyzer
from .surge_filter import SurgeFilter

logger = logging.getLogger(__name__)

class DynamicStockSelector:
    """동적 종목 선정 클래스"""
    
    def __init__(self, api_client: KISAPIClient, config: Dict = None):
        self.api_client = api_client
        self.config = config or {}
        self.valuation_analyzer = ValuationAnalyzer(api_client)
        self.surge_filter = SurgeFilter(api_client)
        self.last_update = None
        self.current_target_stocks = []
        self.update_interval_minutes = 5  # 테스트용 5분마다 업데이트
        
        # 필터링 기준
        self.filters = {
            'min_price': 5000,      # 최소가격 5,000원
            'max_price': 100000,    # 최대가격 100,000원  
            'min_volume': 1000000,  # 최소거래량 100만주
            'min_change_rate': 1.0, # 최소등락률 1%
            'max_stocks': 10        # 최대 선정 종목 수
        }
        
        # 제외할 종목들 (관리종목, 우선주 등)
        self.exclude_patterns = ['K', '9', '0']  # 종목코드 패턴으로 제외
        
    async def should_update_stocks(self) -> bool:
        """종목 목록 업데이트가 필요한지 확인"""
        if not self.last_update:
            return True
            
        now = datetime.now()
        time_diff = (now - self.last_update).total_seconds() / 60
        
        return time_diff >= self.update_interval_minutes
    
    async def get_dynamic_target_stocks(self) -> List[str]:
        """동적으로 거래 대상 종목 선정"""
        try:
            if not await self.should_update_stocks():
                logger.debug(f"Using cached target stocks: {self.current_target_stocks}")
                return self.current_target_stocks
            
            logger.info("🔍 Updating target stocks based on market activity...")
            
            # 먼저 거래량 순위 조회
            logger.info("📊 Fetching volume ranking data...")
            try:
                volume_data = await self.api_client.get_volume_ranking()
                logger.debug(f"Volume ranking response: {volume_data}")
                
                if not volume_data:
                    logger.error("Volume ranking data is None")
                    return ["005930", "000660", "035420"]  # 기본 종목들
                    
                if volume_data.get('rt_cd') != '0':
                    logger.error(f"Volume ranking API failed: rt_cd={volume_data.get('rt_cd')}, msg={volume_data.get('msg1', 'Unknown error')}")
                    return ["005930", "000660", "035420"]  # 기본 종목들
                    
            except Exception as e:
                logger.error(f"Exception during volume ranking fetch: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                return ["005930", "000660", "035420"]  # 기본 종목들
            
            logger.info(f"📈 Volume ranking received: {len(volume_data.get('output', []))} stocks")
            
            # 거래량 데이터 재사용하여 활발한 종목 조회
            logger.info("🔍 Filtering active stocks...")
            active_stocks = await self.api_client.get_active_stocks(
                min_price=self.filters['min_price'],
                max_price=self.filters['max_price'],
                volume_data=volume_data
            )
            
            if active_stocks:
                logger.info(f"✅ Found {len(active_stocks)} active stocks:")
                for i, stock in enumerate(active_stocks[:5], 1):
                    logger.info(f"  {i}. {stock['stock_name']}({stock['stock_code']}): "
                              f"점수 {stock['score']:,.0f} (가격: {stock['current_price']:,.0f}원, "
                              f"등락률: {stock['change_rate']:+.2f}%)")
            else:
                logger.warning("❌ No stocks passed filtering criteria")
            
            if not active_stocks:
                logger.warning("No active stocks found, using default stocks")
                return ["005930", "000660", "035420"]  # 기본 종목들
            
            # 급등주 필터링 먼저 적용
            stock_codes = [stock['stock_code'] for stock in active_stocks]
            surge_config = self.config.get('surge_filter', {})
            
            if surge_config.get('enable_surge_filter', False):
                logger.info("🚫 Applying surge stock filtering...")
                filtered_by_surge = await self.surge_filter.filter_surge_stocks(stock_codes, self.config)
                
                if filtered_by_surge:
                    active_stocks = [s for s in active_stocks if s['stock_code'] in filtered_by_surge]
                    stock_codes = [s['stock_code'] for s in active_stocks]
                    logger.info(f"📊 Surge filtering result: {len(active_stocks)} stocks passed")
                else:
                    logger.warning("🚫 모든 종목이 급등주로 분류되어 제외됨")
                    active_stocks = active_stocks[:3]  # 상위 3개만 유지
                    stock_codes = [s['stock_code'] for s in active_stocks]
            else:
                logger.info("⏭️ Surge filtering disabled")
            
            # 밸류에이션 필터링 적용 (OR 방식으로 변경 - 하나라도 통과하면 OK)
            valuation_config = self.config.get('valuation_filters', {})

            # 각 필터별로 통과한 종목 수집
            passed_stocks_by_filter = {}
            stock_codes = [s['stock_code'] for s in active_stocks]

            # PBR 필터링
            if valuation_config.get('enable_pbr_filter', False):
                logger.info("🔍 Applying PBR filtering...")

                min_pbr = valuation_config.get('min_pbr', 0.1)
                max_pbr = valuation_config.get('max_pbr', 3.0)  # 2.0 → 3.0으로 완화
                require_data = False  # 데이터 필수 조건 완화

                filtered_by_pbr = await self.valuation_analyzer.filter_by_pbr(
                    stock_codes, min_pbr, max_pbr, require_data
                )

                if filtered_by_pbr:
                    passed_stocks_by_filter['pbr'] = set(filtered_by_pbr)
                    logger.info(f"📊 PBR filtering result: {len(filtered_by_pbr)} stocks passed")

            # PER 필터링
            if valuation_config.get('enable_per_filter', False):
                logger.info("🔍 Applying PER filtering...")

                min_per = valuation_config.get('min_per', 3.0)
                max_per = valuation_config.get('max_per', 30.0)  # 20.0 → 30.0으로 완화
                require_data = False  # 데이터 필수 조건 완화

                filtered_by_per = await self.valuation_analyzer.filter_by_per(
                    stock_codes, min_per, max_per, require_data
                )

                if filtered_by_per:
                    passed_stocks_by_filter['per'] = set(filtered_by_per)
                    logger.info(f"📊 PER filtering result: {len(filtered_by_per)} stocks passed")

            # ROE 필터링
            if valuation_config.get('enable_roe_filter', False):
                logger.info("🔍 Applying ROE filtering...")

                min_roe = valuation_config.get('min_roe', 3.0)  # 5.0 → 3.0으로 완화
                require_data = False  # 데이터 필수 조건 완화

                filtered_by_roe = await self.valuation_analyzer.filter_by_roe(
                    stock_codes, min_roe, require_data
                )

                if filtered_by_roe:
                    passed_stocks_by_filter['roe'] = set(filtered_by_roe)
                    logger.info(f"📊 ROE filtering result: {len(filtered_by_roe)} stocks passed")

            # PSR 필터링
            if valuation_config.get('enable_psr_filter', False):
                logger.info("🔍 Applying PSR filtering...")

                max_psr = valuation_config.get('max_psr', 5.0)  # 3.0 → 5.0으로 완화
                require_data = False  # 데이터 필수 조건 완화

                filtered_by_psr = await self.valuation_analyzer.filter_by_psr(
                    stock_codes, max_psr, require_data
                )

                if filtered_by_psr:
                    passed_stocks_by_filter['psr'] = set(filtered_by_psr)
                    logger.info(f"📊 PSR filtering result: {len(filtered_by_psr)} stocks passed")

            # OR 방식으로 통합: 최소 2개 이상의 지표를 통과한 종목만 선택
            if passed_stocks_by_filter:
                min_filters_required = valuation_config.get('min_filters_required', 2)  # 최소 2개 필터 통과
                stock_filter_counts = {}

                for filter_name, passed_codes in passed_stocks_by_filter.items():
                    for code in passed_codes:
                        stock_filter_counts[code] = stock_filter_counts.get(code, 0) + 1

                # 최소 기준 통과한 종목만 유지
                filtered_codes = {code for code, count in stock_filter_counts.items() if count >= min_filters_required}

                if filtered_codes:
                    active_stocks = [s for s in active_stocks if s['stock_code'] in filtered_codes]
                    logger.info(f"✅ 밸류에이션 필터 통과: {len(active_stocks)}개 종목 (최소 {min_filters_required}개 지표 만족)")

                    # 각 종목이 통과한 필터 정보 로깅
                    for stock in active_stocks[:5]:
                        code = stock['stock_code']
                        passed_filters = [name for name, codes in passed_stocks_by_filter.items() if code in codes]
                        logger.info(f"  • {stock['stock_name']}({code}): {', '.join(passed_filters).upper()} 통과")
                else:
                    logger.warning(f"❌ {min_filters_required}개 이상 필터를 통과한 종목 없음, 우회")
            else:
                logger.info("⏭️ 밸류에이션 필터링 전체 비활성화 또는 데이터 없음")
            
            if not active_stocks:
                logger.warning("No stocks remaining after valuation filtering, using defaults")
                return ["005930", "000660", "035420"]
            
            # 최종 종목 선정
            self.current_target_stocks = [stock['stock_code'] for stock in active_stocks]
            self.last_update = datetime.now()
            
            # 선정된 종목 로깅 (밸류에이션 정보 포함)
            await self._log_selected_stocks(active_stocks)
            
            return self.current_target_stocks
                
        except Exception as e:
            logger.error(f"Error updating target stocks: {e}")
            return self.current_target_stocks or ["005930"]
    
    async def _filter_and_rank_stocks(self, stocks_data: List[Dict]) -> List[Dict]:
        """종목 필터링 및 순위 매기기"""
        filtered_stocks = []
        
        for stock in stocks_data:
            try:
                stock_code = stock['stock_code']
                stock_name = stock['stock_name']
                current_price = stock['current_price']
                volume = stock['volume']
                change_rate = stock['change_rate']
                
                # 기본 필터링
                if not self._passes_basic_filters(stock_code, stock_name, current_price, volume, change_rate):
                    continue
                
                # 추가 점수 계산
                score = self._calculate_stock_score(stock)
                stock['final_score'] = score
                
                filtered_stocks.append(stock)
                
            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"Error filtering stock {stock}: {e}")
                continue
        
        # 점수순으로 정렬
        filtered_stocks.sort(key=lambda x: x['final_score'], reverse=True)
        
        # 최대 종목 수만큼 반환
        return filtered_stocks[:self.filters['max_stocks']]
    
    def _passes_basic_filters(self, stock_code: str, stock_name: str, price: float, 
                            volume: int, change_rate: float) -> bool:
        """기본 필터링 조건 확인"""
        # 종목코드 패턴 체크 (관리종목, 우선주 등 제외)
        if any(pattern in stock_code for pattern in self.exclude_patterns):
            return False
        
        # 가격 범위 체크
        if not (self.filters['min_price'] <= price <= self.filters['max_price']):
            return False
        
        # 거래량 체크
        if volume < self.filters['min_volume']:
            return False
        
        # 등락률 체크
        if abs(change_rate) < self.filters['min_change_rate']:
            return False
        
        # 종목명 필터링 (ETF, 리츠 등 제외)
        exclude_names = ['ETF', 'REIT', 'ETN', 'KODEX', 'TIGER', 'KBSTAR']
        if any(exclude in stock_name for exclude in exclude_names):
            return False
        
        return True
    
    def _calculate_stock_score(self, stock: Dict) -> float:
        """종목 점수 계산"""
        try:
            volume = stock['volume']
            change_rate = abs(stock['change_rate'])
            price = stock['current_price']
            
            # 기본 점수 = 거래량 * 등락률
            base_score = volume * change_rate
            
            # 가격대별 보정 (너무 저가주나 고가주는 감점)
            price_factor = 1.0
            if price < 10000:  # 1만원 이하는 감점
                price_factor = 0.8
            elif price > 50000:  # 5만원 이상은 감점
                price_factor = 0.9
            
            # 등락률 보정 (적정 등락률 범위 우대)
            change_factor = 1.0
            if 2.0 <= abs(change_rate) <= 8.0:  # 적정 등락률 범위
                change_factor = 1.2
            elif abs(change_rate) > 15.0:  # 과도한 등락률은 감점
                change_factor = 0.7
            
            final_score = base_score * price_factor * change_factor
            
            return final_score
            
        except (KeyError, ValueError, TypeError):
            return 0.0
    
    def get_current_targets(self) -> List[str]:
        """현재 선정된 대상 종목 반환"""
        return self.current_target_stocks.copy()
    
    def add_manual_stock(self, stock_code: str):
        """수동으로 종목 추가"""
        if stock_code not in self.current_target_stocks:
            self.current_target_stocks.append(stock_code)
            logger.info(f"Manually added stock: {stock_code}")
    
    def remove_manual_stock(self, stock_code: str):
        """수동으로 종목 제거"""
        if stock_code in self.current_target_stocks:
            self.current_target_stocks.remove(stock_code)
            logger.info(f"Manually removed stock: {stock_code}")
    
    def update_filters(self, new_filters: Dict):
        """필터링 기준 업데이트"""
        self.filters.update(new_filters)
        logger.info(f"Updated filters: {self.filters}")
        
        # 필터 변경 시 즉시 업데이트 필요
        self.last_update = None
    
    async def get_stock_summary(self) -> str:
        """현재 대상 종목 요약 정보"""
        if not self.current_target_stocks:
            return "선정된 대상 종목이 없습니다."
        
        summary = f"🎯 현재 대상 종목 ({len(self.current_target_stocks)}개):\n"
        
        for i, stock_code in enumerate(self.current_target_stocks, 1):
            try:
                # 현재가 조회
                price_data = await self.api_client.get_current_price(stock_code)
                if price_data and price_data.get('rt_cd') == '0':
                    output = price_data['output']
                    name = output.get('hts_kor_isnm', stock_code)
                    price = output.get('stck_prpr', '0')
                    change_rate = output.get('prdy_ctrt', '0')
                    
                    summary += f"{i}. {name}({stock_code}): {price}원 ({change_rate}%)\n"
                else:
                    summary += f"{i}. {stock_code}: 정보 조회 실패\n"
                    
            except Exception as e:
                summary += f"{i}. {stock_code}: 오류 ({str(e)[:20]})\n"
        
        if self.last_update:
            summary += f"\n마지막 업데이트: {self.last_update.strftime('%H:%M:%S')}"
        
        return summary
    
    async def _log_selected_stocks(self, active_stocks: List[Dict]):
        """선정된 종목들을 밸류에이션 정보와 함께 로깅"""
        logger.info("📈 Final selected stocks with valuation:")
        
        for i, stock in enumerate(active_stocks[:5], 1):
            stock_code = stock['stock_code']
            stock_name = stock['stock_name']
            
            # 밸류에이션 정보 조회
            try:
                metrics = await self.valuation_analyzer.get_valuation_metrics(stock_code)
                valuation_info = []
                
                if metrics and metrics.pbr is not None:
                    valuation_info.append(f"PBR: {metrics.pbr:.2f}")
                else:
                    valuation_info.append("PBR: N/A")
                
                if metrics and metrics.per is not None:
                    valuation_info.append(f"PER: {metrics.per:.2f}")
                else:
                    valuation_info.append("PER: N/A")
                
                if metrics and metrics.roe is not None:
                    valuation_info.append(f"ROE: {metrics.roe:.2f}%")
                else:
                    valuation_info.append("ROE: N/A")
                
                if metrics and metrics.psr is not None:
                    valuation_info.append(f"PSR: {metrics.psr:.2f}")
                else:
                    valuation_info.append("PSR: N/A")
                
                valuation_str = ", ".join(valuation_info)
            except:
                valuation_str = "PBR: Error, PER: Error, ROE: Error, PSR: Error"
            
            logger.info(f"  {i}. {stock_name}({stock_code}): "
                       f"가격 {stock['current_price']:,.0f}원, "
                       f"등락률 {stock['change_rate']:+.2f}%, "
                       f"{valuation_str}")
    
    def get_valuation_config(self) -> Dict:
        """현재 밸류에이션 필터 설정 반환"""
        return self.config.get('valuation_filters', {})