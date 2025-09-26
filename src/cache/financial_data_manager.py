import asyncio
import logging
from typing import Optional, Dict, List, Tuple
from datetime import datetime
from .financial_data_cache import FinancialDataCache, CachedFinancialData

logger = logging.getLogger(__name__)

class FinancialDataManager:
    """캐싱과 폴백 로직을 포함한 금융 데이터 관리자"""

    def __init__(self, api_client, cache_hours: int = 24):
        self.api_client = api_client
        self.cache = FinancialDataCache(cache_hours=cache_hours)

        # 로그 중복 방지를 위한 캐시
        self._log_suppression_cache = {}
        self._log_suppression_hours = 1  # 1시간 동안 같은 종목의 경고 로그 억제

        # 데이터 부족 종목 블랙리스트 (재무데이터 없는 종목들)
        self._data_poor_stocks = {
            '430690',  # 지속적으로 데이터 부족
            # 필요시 다른 종목들도 추가 가능
        }

        # 데이터 품질 모니터링 통계
        self._quality_stats = {
            'total_requests': 0,
            'api_success': 0,
            'api_failures': 0,
            'fallback_success': 0,
            'default_used': 0,
            'cache_hits': 0,
            'by_metric': {
                'per': {'requests': 0, 'api_success': 0, 'defaults': 0},
                'pbr': {'requests': 0, 'api_success': 0, 'defaults': 0},
                'roe': {'requests': 0, 'api_success': 0, 'defaults': 0},
                'psr': {'requests': 0, 'api_success': 0, 'defaults': 0},
            },
            'frequent_failures': {}  # 종목별 실패 횟수 추적
        }

        # 품질 보고서 출력 간격
        self._quality_report_interval = 100  # 100번 요청마다 리포트 출력
        self._last_quality_report = 0

        # 업종별 기본값 (보수적 추정치)
        self.sector_defaults = {
            'technology': {'per': 25.0, 'roe': 15.0, 'psr': 5.0, 'pbr': 3.0},
            'finance': {'per': 12.0, 'roe': 10.0, 'psr': 1.5, 'pbr': 1.2},
            'manufacturing': {'per': 15.0, 'roe': 8.0, 'psr': 1.8, 'pbr': 1.5},
            'retail': {'per': 20.0, 'roe': 12.0, 'psr': 2.5, 'pbr': 2.0},
            'default': {'per': 20.0, 'roe': 10.0, 'psr': 3.0, 'pbr': 2.0}
        }

    async def get_per_with_fallback(self, stock_code: str) -> Optional[float]:
        """PER 조회 (캐싱 + 폴백 로직)"""
        return await self._get_metric_with_fallback(stock_code, 'per', self._calculate_per_fallbacks)

    async def get_roe_with_fallback(self, stock_code: str) -> Optional[float]:
        """ROE 조회 (캐싱 + 폴백 로직)"""
        return await self._get_metric_with_fallback(stock_code, 'roe', self._calculate_roe_fallbacks)

    async def get_psr_with_fallback(self, stock_code: str) -> Optional[float]:
        """PSR 조회 (캐싱 + 폴백 로직)"""
        return await self._get_metric_with_fallback(stock_code, 'psr', self._calculate_psr_fallbacks)

    async def get_pbr_with_fallback(self, stock_code: str) -> Optional[float]:
        """PBR 조회 (캐싱 + 폴백 로직)"""
        return await self._get_metric_with_fallback(stock_code, 'pbr', self._calculate_pbr_fallbacks)


    async def _calculate_per_fallbacks(self, stock_code: str) -> Optional[float]:
        """PER 폴백 계산 로직들"""
        try:
            # 폴백 1: 다른 API 엔드포인트 시도
            overview_data = await self.api_client.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})

                # EPS로 직접 계산
                eps = output.get('eps')
                if eps:
                    try:
                        price_data = await self.api_client.get_current_price(stock_code)
                        if price_data and price_data.get('rt_cd') == '0':
                            current_price = float(price_data['output'].get('stck_prpr', 0))
                            eps_val = float(eps)
                            if eps_val > 0:
                                per = current_price / eps_val
                                logger.debug(f"PER calculated from EPS: {per:.2f}")
                                return per
                    except:
                        pass

            # 폴백 2: 업종 평균 추정
            sector_avg = await self._estimate_sector_average(stock_code, 'per')
            if sector_avg:
                return sector_avg

            return None

        except Exception as e:
            logger.error(f"Error in PER fallback for {stock_code}: {e}")
            return None

    async def _calculate_roe_fallbacks(self, stock_code: str) -> Optional[float]:
        """ROE 폴백 계산 로직들"""
        try:
            # 폴백 1: PBR과 PER로 추정
            pbr = await self.get_pbr_with_fallback(stock_code)
            per = await self.get_per_with_fallback(stock_code)

            if pbr and per and pbr > 0 and per > 0:
                # 간단한 DuPont 공식 근사
                estimated_roe = (1 / per) * (1 / pbr) * 100
                if 0 < estimated_roe < 50:  # 합리적 범위
                    logger.debug(f"ROE estimated from PBR/PER: {estimated_roe:.2f}%")
                    return estimated_roe

            # 폴백 2: 재무제표 데이터 재시도
            overview_data = await self.api_client.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})

                # 순이익과 자기자본으로 직접 계산
                for net_income_key in ['net_income', 'ni', 'profit']:
                    for equity_key in ['equity', 'stockholders_equity', 'se']:
                        net_income = output.get(net_income_key)
                        equity = output.get(equity_key)

                        if net_income and equity:
                            try:
                                ni_val = float(net_income)
                                eq_val = float(equity)
                                if eq_val > 0:
                                    roe = (ni_val / eq_val) * 100
                                    if -50 < roe < 50:  # 합리적 범위
                                        logger.debug(f"ROE calculated from financials: {roe:.2f}%")
                                        return roe
                            except:
                                continue

            return None

        except Exception as e:
            logger.error(f"Error in ROE fallback for {stock_code}: {e}")
            return None

    async def _calculate_psr_fallbacks(self, stock_code: str) -> Optional[float]:
        """PSR 폴백 계산 로직들"""
        try:
            # 폴백 1: 시가총액과 매출로 직접 계산
            price_data = await self.api_client.get_current_price(stock_code)
            overview_data = await self.api_client.get_stock_overview(stock_code)

            if (price_data and price_data.get('rt_cd') == '0' and
                overview_data and overview_data.get('rt_cd') == '0'):

                current_price = float(price_data['output'].get('stck_prpr', 0))
                output = overview_data.get('output', {})

                shares = output.get('lstg_st_cnt')

                for revenue_key in ['revenue', 'sales', 'total_revenue', 'tr', 'sales_revenue']:
                    revenue = output.get(revenue_key)
                    if shares and revenue and current_price > 0:
                        try:
                            shares_val = float(shares)
                            revenue_val = float(revenue)
                            if revenue_val > 0:
                                market_cap = current_price * shares_val
                                psr = market_cap / revenue_val
                                if 0 < psr < 20:  # 합리적 범위
                                    logger.debug(f"PSR calculated from market cap/revenue: {psr:.2f}")
                                    return psr
                        except:
                            continue

            # 폴백 2: PER 기반 PSR 추정
            per = await self.get_per_with_fallback(stock_code)
            if per and per > 0:
                # 업종별 평균 순이익률 추정
                sector_net_margin = self._estimate_sector_net_margin(stock_code)
                if sector_net_margin:
                    estimated_psr = per * sector_net_margin
                    if 0 < estimated_psr < 15:  # 합리적 범위
                        logger.debug(f"PSR estimated from PER × Net Margin: {estimated_psr:.2f}")
                        return estimated_psr

            # 폴백 3: PBR과 ROE 기반 추정
            pbr = await self.get_pbr_with_fallback(stock_code)
            roe = await self.get_roe_with_fallback(stock_code)

            if pbr and roe and pbr > 0 and roe > 0:
                # PSR ≈ PBR × (ROE/100) × 추정 순이익률
                estimated_net_margin = 0.05  # 5% 기본 순이익률
                estimated_psr = pbr * (roe / 100) * (1 / estimated_net_margin)
                if 0 < estimated_psr < 12:  # 합리적 범위
                    logger.debug(f"PSR estimated from PBR×ROE: {estimated_psr:.2f}")
                    return estimated_psr

            # 폴백 4: 업종 분석 기반 동적 추정
            dynamic_psr = await self._estimate_dynamic_psr(stock_code)
            if dynamic_psr:
                return dynamic_psr

            return None

        except Exception as e:
            logger.error(f"Error in PSR fallback for {stock_code}: {e}")
            return None

    async def _calculate_pbr_fallbacks(self, stock_code: str) -> Optional[float]:
        """PBR 폴백 계산 로직들"""
        try:
            # 기본 API가 실패했다면 다른 엔드포인트 시도
            overview_data = await self.api_client.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})

                # 장부가치로 직접 계산
                bps = output.get('bps')  # Book value Per Share
                if bps:
                    try:
                        price_data = await self.api_client.get_current_price(stock_code)
                        if price_data and price_data.get('rt_cd') == '0':
                            current_price = float(price_data['output'].get('stck_prpr', 0))
                            bps_val = float(bps)
                            if bps_val > 0:
                                pbr = current_price / bps_val
                                logger.debug(f"PBR calculated from BPS: {pbr:.2f}")
                                return pbr
                    except:
                        pass

            return None

        except Exception as e:
            logger.error(f"Error in PBR fallback for {stock_code}: {e}")
            return None

    async def _estimate_sector_average(self, stock_code: str, metric: str) -> Optional[float]:
        """업종 평균 추정 (향후 확장 가능)"""
        # 현재는 단순히 기본값 반환, 나중에 업종 분류 로직 추가 가능
        return None

    def _get_sector_default(self, stock_code: str, metric: str) -> float:
        """업종별 기본값 반환 (강화된 분류 로직)"""
        defaults = self.sector_defaults['default']

        # 업종별 분류 로직 강화
        if stock_code.startswith(('005', '000')):  # 대형주
            if stock_code in ['005930', '000660']:  # 삼성전자, SK하이닉스
                defaults = self.sector_defaults['technology']
            else:
                defaults = self.sector_defaults['manufacturing']

        # 금융업 (은행, 증권, 보험)
        elif (stock_code.endswith(('5', '6', '0')) and len(stock_code) == 6 and
              stock_code.startswith(('0', '1', '2'))):
            defaults = self.sector_defaults['finance']

        # 기술주 분류 확대
        elif stock_code.startswith(('035', '036', '037', '034')):
            defaults = self.sector_defaults['technology']

        # 제약/바이오
        elif stock_code.startswith(('090', '091', '092', '326')):
            # 제약업 특수 지표 설정
            pharma_defaults = {
                'per': 30.0,  # 제약업은 높은 PER
                'roe': 12.0,
                'psr': 5.0,   # 높은 PSR
                'pbr': 2.5
            }
            return pharma_defaults.get(metric, self.sector_defaults['default'][metric])

        # 화학/소재
        elif stock_code.startswith(('001', '002', '003', '004')):
            defaults = {
                'per': 12.0,  # 낮은 PER
                'roe': 6.0,
                'psr': 1.2,
                'pbr': 1.0    # 낮은 PBR
            }

        # 유통/소비재
        elif stock_code.startswith(('008', '009', '021', '023')):
            defaults = self.sector_defaults['retail']

        # 시가총액 고려 조정
        price_adjustment = self._get_market_cap_adjustment(stock_code)
        adjusted_value = defaults.get(metric, self.sector_defaults['default'][metric])

        return adjusted_value * price_adjustment

    def _get_market_cap_adjustment(self, stock_code: str) -> float:
        """시가총액 기반 조정 계수"""
        # 대형주 (005xxx, 000xxx 등)
        if stock_code.startswith(('005', '000', '066')):
            return 0.9  # 보수적 지표
        # 중형주
        elif stock_code.startswith(('01', '02', '03')):
            return 1.0  # 기본값
        # 소형주
        else:
            return 1.2  # 프리미엄 적용

    def _is_valid_metric(self, metric: str, value: float) -> bool:
        """메트릭 값의 유효성 검증 (강화된 로직)"""
        if value is None:
            return False

        # ROE는 음수 가능 (적자 기업)
        if metric != 'roe' and value <= 0:
            return False

        # 업종별 차별화된 범위 설정
        if metric == 'per':
            # 제약/바이오: 높은 PER 허용
            if hasattr(self, '_current_stock_code'):
                stock_code = self._current_stock_code
                if stock_code.startswith(('090', '091', '092', '326')):
                    return 0.1 <= value <= 500.0  # 제약업 확장 범위
                elif stock_code.startswith(('035', '036', '034')):  # 기술주
                    return 0.1 <= value <= 300.0
                else:
                    return 0.1 <= value <= 150.0  # 일반 기업

        elif metric == 'roe':
            return -100.0 <= value <= 150.0  # 적자 ~ 초고수익 범위

        elif metric == 'psr':
            return 0.01 <= value <= 30.0  # PSR 범위 확장

        elif metric == 'pbr':
            return 0.01 <= value <= 15.0  # PBR 범위

        return True  # 기본적으로 유효

    async def _get_metric_with_fallback(self, stock_code: str, metric: str, fallback_func) -> Optional[float]:
        """메트릭 조회 공통 로직 (캐싱 + 폴백) - 개선된 버전"""
        try:
            # 통계 업데이트
            self._quality_stats['total_requests'] += 1
            self._quality_stats['by_metric'][metric]['requests'] += 1

            # 주기적 품질 보고서 출력
            if (self._quality_stats['total_requests'] - self._last_quality_report) >= self._quality_report_interval:
                self.log_quality_summary()
                self._last_quality_report = self._quality_stats['total_requests']

                # 블랙리스트 후보 자동 검사
                candidates = self.get_blacklist_candidates()
                if candidates:
                    logger.info(f"Blacklist candidates found: {candidates}")
                    for candidate in candidates:
                        if self.should_add_to_blacklist(candidate):
                            self.add_to_blacklist(candidate, "Auto-detected frequent failures")

            # 현재 종목 코드 저장 (유효성 검증용)
            self._current_stock_code = stock_code

            # 1. 캐시에서 확인
            cached_data = self.cache.get_cached_data(stock_code)
            if cached_data and getattr(cached_data, metric) is not None:
                value = getattr(cached_data, metric)
                if self._is_valid_metric(metric, value):
                    logger.debug(f"Cache hit - {stock_code} {metric.upper()}: {value}")
                    self._quality_stats['cache_hits'] += 1
                    return value
                else:
                    logger.warning(f"Invalid cached value - {stock_code} {metric.upper()}: {value}, recalculating")

            logger.debug(f"Cache miss for {stock_code} {metric.upper()}, trying API...")

            # 2. 블랙리스트 확인 후 API 호출
            if stock_code in self._data_poor_stocks:
                logger.debug(f"Skipping API for blacklisted stock {stock_code}, using default")
                direct_value = None
            else:
                api_method = getattr(self.api_client, f'calculate_{metric}')
                direct_value = await api_method(stock_code)

                if direct_value is not None and self._is_valid_metric(metric, direct_value):
                    logger.info(f"API success - {stock_code} {metric.upper()}: {direct_value}")
                    self._quality_stats['api_success'] += 1
                    self._quality_stats['by_metric'][metric]['api_success'] += 1
                    self.cache.update_metric(stock_code, metric, direct_value)
                    return direct_value

            # API 실패 통계
            self._quality_stats['api_failures'] += 1
            self._track_failure(stock_code, metric)

            if self._should_log_warning(stock_code, metric, "api_failed"):
                logger.warning(f"API failed for {stock_code} {metric.upper()}, trying fallbacks...")

            # 3. 폴백 로직 시도 (블랙리스트 종목은 건너뛰기)
            if stock_code not in self._data_poor_stocks:
                fallback_value = await fallback_func(stock_code)
                if fallback_value is not None and self._is_valid_metric(metric, fallback_value):
                    logger.info(f"Fallback success - {stock_code} {metric.upper()}: {fallback_value}")
                    self._quality_stats['fallback_success'] += 1
                    self.cache.update_metric(stock_code, metric, fallback_value)
                    return fallback_value

            # 4. 마지막 수단: 업종 기본값 (유효성 검증 포함)
            default_value = self._get_sector_default(stock_code, metric)
            if self._is_valid_metric(metric, default_value):
                if self._should_log_warning(stock_code, metric, "using_default"):
                    logger.warning(f"Using validated default - {stock_code} {metric.upper()}: {default_value}")
                self._quality_stats['default_used'] += 1
                self._quality_stats['by_metric'][metric]['defaults'] += 1
                return default_value
            else:
                # 기본값도 무효하면 최후 수단
                emergency_value = self._get_emergency_default(metric)
                logger.error(f"Using emergency default - {stock_code} {metric.upper()}: {emergency_value}")
                self._quality_stats['default_used'] += 1
                self._quality_stats['by_metric'][metric]['defaults'] += 1
                return emergency_value

        except Exception as e:
            logger.error(f"Error getting {metric} for {stock_code}: {e}")
            self._quality_stats['default_used'] += 1
            self._quality_stats['by_metric'][metric]['defaults'] += 1
            return self._get_emergency_default(metric)
        finally:
            # 정리
            if hasattr(self, '_current_stock_code'):
                delattr(self, '_current_stock_code')

    def _get_emergency_default(self, metric: str) -> float:
        """최후 수단 기본값"""
        emergency_defaults = {
            'per': 20.0,
            'roe': 8.0,
            'psr': 2.0,
            'pbr': 1.5
        }
        return emergency_defaults.get(metric, 1.0)

    def _should_log_warning(self, stock_code: str, metric: str, log_type: str) -> bool:
        """로그 중복 방지를 위한 체크"""
        from datetime import datetime, timedelta

        cache_key = f"{stock_code}_{metric}_{log_type}"
        current_time = datetime.now()

        if cache_key in self._log_suppression_cache:
            last_log_time = self._log_suppression_cache[cache_key]
            if current_time - last_log_time < timedelta(hours=self._log_suppression_hours):
                return False

        # 로그 기록
        self._log_suppression_cache[cache_key] = current_time
        return True

    def get_cache_stats(self) -> Dict:
        """캐시 통계 조회"""
        return self.cache.get_cache_stats()

    def cleanup_cache(self):
        """만료된 캐시 정리"""
        self.cache.cleanup_expired()

    def _estimate_sector_net_margin(self, stock_code: str) -> Optional[float]:
        """업종별 순이익률 추정"""
        # 업종별 평균 순이익률
        sector_margins = {
            'technology': 0.12,      # 12%
            'finance': 0.25,         # 25% (금융업 특성)
            'manufacturing': 0.06,   # 6%
            'retail': 0.04,          # 4%
            'pharmaceutical': 0.15,  # 15%
            'chemical': 0.08,        # 8%
            'default': 0.08          # 8%
        }

        # 종목코드 기반 업종 추정
        if stock_code.startswith(('005', '000')):
            return sector_margins['manufacturing']
        elif stock_code.endswith(('5', '6')) and len(stock_code) == 6:
            return sector_margins['finance']
        elif stock_code.startswith('035'):
            return sector_margins['technology']
        elif stock_code.startswith(('090', '091')):  # 제약
            return sector_margins['pharmaceutical']
        elif stock_code.startswith(('001', '002')):  # 화학
            return sector_margins['chemical']
        else:
            return sector_margins['default']

    async def _estimate_dynamic_psr(self, stock_code: str) -> Optional[float]:
        """동적 PSR 추정 (시장 상황 고려)"""
        try:
            # 현재가 정보 확인
            price_data = await self.api_client.get_current_price(stock_code)
            if not price_data or price_data.get('rt_cd') != '0':
                return None

            output = price_data.get('output', {})
            current_price = float(output.get('stck_prpr', 0))

            # 거래량 정보로 시장 관심도 측정
            volume = float(output.get('acml_vol', 0))
            avg_volume = float(output.get('avg_vol_5d', volume))  # 5일 평균 대비

            if current_price <= 0:
                return None

            # 가격대별 PSR 추정
            if current_price < 10000:  # 소형주
                base_psr = 2.0
            elif current_price < 50000:  # 중형주
                base_psr = 1.5
            else:  # 대형주
                base_psr = 1.0

            # 거래량 기반 조정
            if avg_volume > 0 and volume > avg_volume * 2:  # 거래량 급증
                base_psr *= 1.2  # 프리미엄 적용

            # 업종별 조정
            sector_multiplier = 1.0
            if stock_code.startswith('035'):  # 기술주
                sector_multiplier = 1.5
            elif stock_code.startswith(('090', '091')):  # 제약
                sector_multiplier = 2.0

            estimated_psr = base_psr * sector_multiplier

            if 0.5 <= estimated_psr <= 8.0:  # 합리적 범위
                logger.debug(f"Dynamic PSR estimate for {stock_code}: {estimated_psr:.2f}")
                return estimated_psr

            return None

        except Exception as e:
            logger.error(f"Error in dynamic PSR estimation for {stock_code}: {e}")
            return None

    def _track_failure(self, stock_code: str, metric: str):
        """실패한 종목-메트릭 조합 추적"""
        key = f"{stock_code}_{metric}"
        if key not in self._quality_stats['frequent_failures']:
            self._quality_stats['frequent_failures'][key] = 0
        self._quality_stats['frequent_failures'][key] += 1

        # 실패 횟수가 많은 종목은 블랙리스트 후보로 고려
        if self._quality_stats['frequent_failures'][key] >= 5:
            logger.warning(f"Stock {stock_code} has failed {metric} API {self._quality_stats['frequent_failures'][key]} times")

    def get_quality_report(self) -> Dict:
        """데이터 품질 보고서 생성"""
        stats = self._quality_stats.copy()

        # 비율 계산
        total = stats['total_requests']
        if total > 0:
            stats['success_rate'] = (stats['api_success'] + stats['fallback_success']) / total * 100
            stats['api_success_rate'] = stats['api_success'] / total * 100
            stats['cache_hit_rate'] = stats['cache_hits'] / total * 100
            stats['default_usage_rate'] = stats['default_used'] / total * 100

            # 메트릭별 성공률
            for metric, data in stats['by_metric'].items():
                if data['requests'] > 0:
                    data['api_success_rate'] = data['api_success'] / data['requests'] * 100
                    data['default_usage_rate'] = data['defaults'] / data['requests'] * 100

        # 자주 실패하는 종목 TOP 5
        frequent_failures = sorted(
            stats['frequent_failures'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        stats['top_failures'] = frequent_failures

        return stats

    def log_quality_summary(self):
        """품질 통계 요약 로그 출력"""
        report = self.get_quality_report()

        logger.info("=== Financial Data Quality Report ===")
        logger.info(f"Total Requests: {report['total_requests']}")

        if 'success_rate' in report:
            logger.info(f"Overall Success Rate: {report['success_rate']:.1f}%")
            logger.info(f"API Success Rate: {report['api_success_rate']:.1f}%")
            logger.info(f"Cache Hit Rate: {report['cache_hit_rate']:.1f}%")
            logger.info(f"Default Usage Rate: {report['default_usage_rate']:.1f}%")

        logger.info("--- By Metric ---")
        for metric, data in report['by_metric'].items():
            if 'api_success_rate' in data:
                logger.info(f"{metric.upper()}: API Success {data['api_success_rate']:.1f}%, Default Usage {data['default_usage_rate']:.1f}%")

        if report['top_failures']:
            logger.info("--- Top Failures ---")
            for failure_key, count in report['top_failures']:
                stock_code, metric = failure_key.split('_')
                logger.info(f"{stock_code} ({metric.upper()}): {count} failures")

    def should_add_to_blacklist(self, stock_code: str, threshold: int = 10) -> bool:
        """종목을 블랙리스트에 추가할지 판단"""
        total_failures = sum(
            count for key, count in self._quality_stats['frequent_failures'].items()
            if key.startswith(f"{stock_code}_")
        )
        return total_failures >= threshold

    def add_to_blacklist(self, stock_code: str, reason: str = "Frequent API failures"):
        """종목을 블랙리스트에 추가"""
        if stock_code not in self._data_poor_stocks:
            self._data_poor_stocks.add(stock_code)
            logger.warning(f"Added {stock_code} to blacklist: {reason}")

    def get_blacklist_candidates(self, threshold: int = 5) -> List[str]:
        """블랙리스트 후보 종목들 반환"""
        candidates = []
        stock_failure_counts = {}

        # 종목별 총 실패 횟수 계산
        for key, count in self._quality_stats['frequent_failures'].items():
            if '_' in key:
                stock_code, _ = key.split('_', 1)
                if stock_code not in stock_failure_counts:
                    stock_failure_counts[stock_code] = 0
                stock_failure_counts[stock_code] += count

        # 임계값 초과 종목들
        for stock_code, total_failures in stock_failure_counts.items():
            if total_failures >= threshold and stock_code not in self._data_poor_stocks:
                candidates.append(stock_code)

        return candidates