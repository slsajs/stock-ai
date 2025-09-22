#!/usr/bin/env python3
"""
강화된 손절 시스템
API 지연과 무관하게 즉시 손절을 실행하는 시스템
"""

import logging
import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import threading

@dataclass
class PositionInfo:
    """포지션 정보"""
    stock_code: str
    stock_name: str
    quantity: int
    avg_price: float
    current_price: float
    purchase_time: datetime
    stop_loss_price: float
    take_profit_price: float
    trailing_stop_price: Optional[float] = None
    max_price_seen: float = 0.0
    volatility: float = 0.0  # 변동성 지표 추가
    
class EnhancedStopLossManager:
    """강화된 손절 관리자"""
    
    def __init__(self, api_client, config: Dict = None):
        self.api_client = api_client
        self.config = config or {}
        self.positions: Dict[str, PositionInfo] = {}
        self.logger = logging.getLogger(__name__)
        
        # 손절 설정 - 변동성 기반 동적 조정
        stop_loss_config = self.config.get('enhanced_stop_loss', {})
        self.base_stop_loss_pct = stop_loss_config.get('stop_loss_pct', 1.5)  # 기본 1.5%
        self.volatility_multiplier = stop_loss_config.get('volatility_multiplier', 1.5)  # 변동성 승수
        self.take_profit_pct = stop_loss_config.get('take_profit_pct', 3.0)
        self.base_trailing_stop_pct = stop_loss_config.get('trailing_stop_pct', 1.0)  # 기본 트레일링 1.0%
        
        # 즉시 실행 설정
        self.force_execution = stop_loss_config.get('force_execution', True)
        self.max_execution_delay = stop_loss_config.get('max_execution_delay_seconds', 5)
        
        # 실행 큐
        self.execution_queue = asyncio.Queue()
        self.execution_lock = asyncio.Lock()
        self.is_running = False
        
        self.logger.info(f"Enhanced StopLoss Manager 초기화:")
        self.logger.info(f"  • 기본 손절: {self.base_stop_loss_pct}%")
        self.logger.info(f"  • 익절: {self.take_profit_pct}%")
        self.logger.info(f"  • 기본 트레일링: {self.base_trailing_stop_pct}%")
        self.logger.info(f"  • 변동성 승수: {self.volatility_multiplier}")
        self.logger.info(f"  • 강제실행: {self.force_execution}")

    def _calculate_volatility(self, stock_code: str, current_price: float) -> float:
        """종목의 변동성 계산 (간단한 ATR 기반)"""
        try:
            # 실제 구현에서는 ATR 등을 사용하지만, 여기서는 간단히 구현
            if stock_code not in self.positions:
                return 0.02  # 기본 2% 변동성

            position = self.positions[stock_code]
            # 매수가 대비 현재 변동폭을 기준으로 변동성 계산
            price_change_pct = abs(current_price - position.avg_price) / position.avg_price

            # 변동성 = 최근 변동폭의 평균 (간단히 현재 변동폭 사용)
            volatility = max(0.01, min(0.1, price_change_pct * 2))  # 1%~10% 범위

            return volatility
        except Exception as e:
            self.logger.error(f"변동성 계산 오류 {stock_code}: {e}")
            return 0.02

    def _get_dynamic_stop_loss_pct(self, stock_code: str, current_price: float) -> float:
        """변동성 기반 동적 손절률 계산"""
        volatility = self._calculate_volatility(stock_code, current_price)
        dynamic_stop_loss = self.base_stop_loss_pct * (1 + volatility * self.volatility_multiplier)

        # 최소 1%, 최대 5% 제한
        dynamic_stop_loss = max(1.0, min(5.0, dynamic_stop_loss))

        self.logger.debug(f"{stock_code} 동적 손절률: {dynamic_stop_loss:.2f}% (변동성: {volatility:.3f})")
        return dynamic_stop_loss

    def _get_dynamic_trailing_stop_pct(self, stock_code: str, current_price: float) -> float:
        """최적화된 동적 트레일링스탑률 계산"""
        if stock_code not in self.positions:
            return self.base_trailing_stop_pct

        position = self.positions[stock_code]

        # 1. 현재 수익률 계산
        profit_pct = ((current_price - position.avg_price) / position.avg_price) * 100

        # 2. 보유 시간 계산 (분)
        from datetime import datetime
        holding_minutes = (datetime.now() - position.purchase_time).total_seconds() / 60

        # 3. 수익률 기반 트레일링스탑 (가장 중요)
        if profit_pct < 0.5:
            # 수익이 거의 없을 때는 트레일링 비활성화
            profit_based_trailing = 99.0  # 사실상 비활성화
        elif profit_pct < 1.0:
            profit_based_trailing = 2.8  # 매우 완화
        elif profit_pct < 2.0:
            profit_based_trailing = 2.2  # 완화
        elif profit_pct < 3.0:
            profit_based_trailing = 1.8  # 기본
        else:
            profit_based_trailing = 1.5  # 큰 수익일 때 강화

        # 4. 시간 기반 완화 (초기 변동성 고려) - 더 세밀한 조정
        if holding_minutes < 2:
            time_based_adjustment = 1.5  # 초기 2분은 매우 완화
        elif holding_minutes < 5:
            time_based_adjustment = 1.0  # 5분까지 완화
        elif holding_minutes < 10:
            time_based_adjustment = 0.8  # 10분까지 점진적 완화
        elif holding_minutes < 15:
            time_based_adjustment = 0.6  # 15분까지 완화
        elif holding_minutes < 25:
            time_based_adjustment = 0.4  # 25분까지 완화
        elif holding_minutes < 40:
            time_based_adjustment = 0.2  # 40분까지 소폭 완화
        else:
            # 장시간 보유 시 시장 상황에 따른 조정
            current_hour = datetime.now().hour
            if current_hour >= 14:  # 장 마감 1시간 전
                time_based_adjustment = -0.3  # 강화로 리스크 관리
            else:
                time_based_adjustment = 0.0  # 완화 없음

        # 5. 변동성 기반 조정
        volatility = self._calculate_volatility(stock_code, current_price)
        volatility_adjustment = volatility * 0.5  # 변동성 50% 반영

        # 6. 종목별 특성 조정
        stock_adjustment = self._get_stock_specific_adjustment(stock_code, current_price)

        # 7. 급격한 가격 변동 조정
        rapid_movement_adjustment = self._handle_rapid_price_movement(stock_code, current_price)

        # 8. 최종 트레일링스탑 계산
        final_trailing = (profit_based_trailing + time_based_adjustment +
                         volatility_adjustment + stock_adjustment + rapid_movement_adjustment)

        # 9. 합리적 범위로 제한
        final_trailing = max(1.0, min(5.0, final_trailing))

        self.logger.debug(f"{stock_code} 최적화 트레일링: {final_trailing:.2f}% "
                         f"(수익률:{profit_pct:.1f}%, 시간:{holding_minutes:.0f}분, 변동성:{volatility:.3f}, "
                         f"조정: 시간+{time_based_adjustment:.1f}% 종목+{stock_adjustment:.1f}% "
                         f"급변동+{rapid_movement_adjustment:.1f}%)")

        return final_trailing

    def _get_stock_specific_adjustment(self, stock_code: str, current_price: float) -> float:
        """종목별 특성을 고려한 트레일링스탑 조정"""
        adjustment = 0.0

        # 1. 가격대별 조정
        if current_price < 5000:  # 소액주
            adjustment += 0.3  # 변동성이 높으므로 완화
        elif current_price < 10000:  # 중저가주
            adjustment += 0.2
        elif current_price > 50000:  # 고가주 (대형주)
            adjustment -= 0.2  # 안정적이므로 강화

        # 2. 종목코드별 특성 (과거 패턴 기반)
        if stock_code.startswith('005'):  # 삼성 계열
            adjustment -= 0.1  # 안정적
        elif stock_code.startswith('000'):  # SK 계열
            adjustment -= 0.1  # 안정적
        elif stock_code.startswith('035'):  # 네이버 등 IT
            adjustment += 0.2  # 변동성 높음
        elif stock_code.startswith(('20', '30', '31', '32')):  # 코스닥
            adjustment += 0.3  # 변동성 매우 높음

        # 3. 특정 문제 종목 개별 조정 (최근 손실 패턴 기반)
        problem_stocks = {
            '317830': 0.8,  # 연속 손실로 대폭 완화 필요
            '201490': 0.4,  # 중간 완화
            '462860': 0.3,  # 소액주 특성으로 완화
            '293490': 0.3,  # 카카오 게임즈 - 변동성 고려
            '035900': 0.2,  # JYP 엔터 - 엔터테인먼트 특성
        }

        if stock_code in problem_stocks:
            adjustment += problem_stocks[stock_code]
            self.logger.debug(f"{stock_code} 문제 종목 조정: +{problem_stocks[stock_code]:.1f}%")

        return adjustment

    def _calculate_rsi_based_adjustment(self, stock_code: str, current_price: float) -> float:
        """RSI 기반 트레일링스탑 조정 (향후 RSI 데이터 연동 시 사용)"""
        # 현재는 기본값 반환, 향후 RSI 데이터와 연동
        return 0.0

    def _handle_rapid_price_movement(self, stock_code: str, current_price: float) -> float:
        """급격한 가격 변동 시 트레일링스탑 조정"""
        if stock_code not in self.positions:
            return 0.0

        position = self.positions[stock_code]

        # 1분 내 급격한 변동 감지 (간단한 구현)
        price_change_pct = abs((current_price - position.avg_price) / position.avg_price) * 100

        adjustment = 0.0

        # 급격한 상승 시 (5% 이상)
        if price_change_pct > 5.0 and current_price > position.avg_price:
            adjustment += 0.5  # 트레일링 완화로 이익 보호
            self.logger.debug(f"{stock_code} 급등 감지: +0.5% 트레일링 완화")

        # 급격한 하락 시 (3% 이상)
        elif price_change_pct > 3.0 and current_price < position.avg_price:
            adjustment -= 0.3  # 트레일링 강화로 손실 제한
            self.logger.debug(f"{stock_code} 급락 감지: -0.3% 트레일링 강화")

        return adjustment

    async def start_monitoring(self):
        """모니터링 시작"""
        if self.is_running:
            return
            
        self.is_running = True
        self.logger.info("🚀 Enhanced StopLoss monitoring started")
        
        # 백그라운드에서 즉시 실행 태스크 시작
        asyncio.create_task(self._immediate_execution_worker())
    
    async def stop_monitoring(self):
        """모니터링 중지"""
        self.is_running = False
        self.logger.info("🛑 Enhanced StopLoss monitoring stopped")
    
    async def add_position(self, stock_code: str, stock_name: str, quantity: int, 
                          avg_price: float, current_price: float = None):
        """포지션 추가"""
        if current_price is None:
            current_price = avg_price
            
        # 손절/익절가 계산 - 동적 손절률 사용
        dynamic_stop_loss_pct = self._get_dynamic_stop_loss_pct(stock_code, current_price)
        stop_loss_price = avg_price * (1 - dynamic_stop_loss_pct / 100)
        take_profit_price = avg_price * (1 + self.take_profit_pct / 100)
        
        position = PositionInfo(
            stock_code=stock_code,
            stock_name=stock_name,
            quantity=quantity,
            avg_price=avg_price,
            current_price=current_price,
            purchase_time=datetime.now(),
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            max_price_seen=current_price
        )
        
        self.positions[stock_code] = position
        
        self.logger.info(f"📍 Enhanced Position added: {stock_name}({stock_code}) "
                        f"{quantity}주 @{avg_price:,.0f}원")
        self.logger.info(f"   손절가: {stop_loss_price:,.0f}원 ({dynamic_stop_loss_pct:.2f}%)")
        self.logger.info(f"   익절가: {take_profit_price:,.0f}원 ({self.take_profit_pct}%)")
    
    async def update_price(self, stock_code: str, current_price: float):
        """실시간 가격 업데이트 및 손절/익절 체크"""
        if stock_code not in self.positions:
            return False
            
        position = self.positions[stock_code]
        position.current_price = current_price
        
        # 최고가 업데이트 (트레일링 스톱용)
        if current_price > position.max_price_seen:
            position.max_price_seen = current_price
            # 트레일링 스톱 가격 업데이트 (개선된 조건)
            profit_pct = ((current_price - position.avg_price) / position.avg_price) * 100

            # 수익률 기반 트레일링 활성화 조건
            if profit_pct > 0.3:  # 0.3% 이상 수익 시 트레일링 활성화
                trailing_stop_pct = self._get_dynamic_trailing_stop_pct(position.stock_code, current_price)

                # 수익이 적을 때는 트레일링 비활성화
                if trailing_stop_pct < 50:  # 99.0이 아닌 정상적인 값일 때만
                    position.trailing_stop_price = current_price * (1 - trailing_stop_pct / 100)
                    self.logger.debug(f"{position.stock_code} 트레일링 업데이트: {trailing_stop_pct:.2f}% "
                                    f"(현재가: {current_price}, 트레일링가: {position.trailing_stop_price:.0f})")
        
        # 손익률 계산
        profit_loss_pct = ((current_price - position.avg_price) / position.avg_price) * 100
        
        # 즉시 실행이 필요한 상황들
        immediate_action = None
        
        # 1. 손절 체크
        if current_price <= position.stop_loss_price:
            immediate_action = ('STOP_LOSS', f"손절 {profit_loss_pct:.2f}%")
            
        # 2. 트레일링 스톱 체크
        elif position.trailing_stop_price and current_price <= position.trailing_stop_price:
            immediate_action = ('TRAILING_STOP', f"트레일링스톱 {profit_loss_pct:.2f}%")
            
        # 3. 익절 체크
        elif current_price >= position.take_profit_price:
            immediate_action = ('TAKE_PROFIT', f"익절 {profit_loss_pct:.2f}%")
        
        # 즉시 실행 큐에 추가
        if immediate_action:
            await self._queue_immediate_execution(stock_code, immediate_action, current_price)
            return True
            
        return False
    
    async def _queue_immediate_execution(self, stock_code: str, action_info: Tuple[str, str], 
                                       current_price: float):
        """즉시 실행 큐에 추가"""
        execution_item = {
            'stock_code': stock_code,
            'action_type': action_info[0],
            'reason': action_info[1],
            'price': current_price,
            'timestamp': datetime.now()
        }
        
        try:
            await asyncio.wait_for(self.execution_queue.put(execution_item), timeout=1.0)
            position = self.positions[stock_code]
            self.logger.warning(f"🚨 즉시실행 대기열 추가: {position.stock_name}({stock_code}) "
                              f"{action_info[1]} @{current_price:,.0f}원")
        except asyncio.TimeoutError:
            self.logger.error(f"⚠️ 실행 큐 오버플로우: {stock_code}")
    
    async def _immediate_execution_worker(self):
        """즉시 실행 워커"""
        while self.is_running:
            try:
                # 대기열에서 실행할 항목 가져오기
                execution_item = await asyncio.wait_for(
                    self.execution_queue.get(), timeout=1.0
                )
                
                # 즉시 실행
                await self._execute_immediate_sell(execution_item)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"즉시 실행 워커 오류: {e}")
                await asyncio.sleep(0.1)
    
    async def _execute_immediate_sell(self, execution_item: Dict):
        """즉시 매도 실행"""
        stock_code = execution_item['stock_code']
        action_type = execution_item['action_type']
        reason = execution_item['reason']
        price = execution_item['price']
        
        if stock_code not in self.positions:
            return
            
        position = self.positions[stock_code]
        
        async with self.execution_lock:
            try:
                self.logger.warning(f"🔥 즉시매도 실행: {position.stock_name}({stock_code}) "
                                  f"{reason} @{price:,.0f}원")
                
                # 즉시 매도 주문 (시장가)
                sell_result = await self._execute_market_sell(stock_code, position.quantity, reason)
                
                if sell_result:
                    # 손익 계산
                    profit_loss = (price - position.avg_price) * position.quantity
                    profit_loss_pct = ((price - position.avg_price) / position.avg_price) * 100
                    
                    self.logger.warning(f"✅ 즉시매도 완료: {position.stock_name}({stock_code}) "
                                      f"{position.quantity}주 @{price:,.0f}원, "
                                      f"손익: {profit_loss:,.0f}원 ({profit_loss_pct:.2f}%)")
                    
                    # 포지션 제거
                    del self.positions[stock_code]
                    
                    return True
                else:
                    self.logger.error(f"❌ 즉시매도 실패: {stock_code}")
                    return False
                    
            except Exception as e:
                self.logger.error(f"즉시매도 실행 오류 {stock_code}: {e}")
                return False
    
    async def _execute_market_sell(self, stock_code: str, quantity: int, reason: str) -> bool:
        """시장가 매도 실행"""
        try:
            # 강제 시장가 매도 (API 지연 무관)
            if self.force_execution:
                self.logger.info(f"🚀 강제 시장가 매도 실행: {stock_code} {quantity}주")
                
                # 최대 3번 재시도
                for attempt in range(3):
                    try:
                        # 시장가 매도 주문
                        result = await asyncio.wait_for(
                            self.api_client.sell_stock_market_order(stock_code, quantity),
                            timeout=self.max_execution_delay
                        )
                        
                        if result and result.get('rt_cd') == '0':
                            return True
                        else:
                            self.logger.warning(f"매도 주문 실패 (시도 {attempt + 1}/3): {result}")
                            
                    except asyncio.TimeoutError:
                        self.logger.warning(f"매도 주문 타임아웃 (시도 {attempt + 1}/3)")
                        
                    except Exception as e:
                        self.logger.error(f"매도 주문 오류 (시도 {attempt + 1}/3): {e}")
                    
                    # 재시도 전 짧은 대기
                    if attempt < 2:
                        await asyncio.sleep(0.5)
            
            return False
            
        except Exception as e:
            self.logger.error(f"시장가 매도 실행 오류: {e}")
            return False
    
    def get_positions_summary(self) -> Dict:
        """포지션 요약"""
        summary = {
            'total_positions': len(self.positions),
            'positions': []
        }
        
        total_profit_loss = 0
        
        for stock_code, position in self.positions.items():
            profit_loss = (position.current_price - position.avg_price) * position.quantity
            profit_loss_pct = ((position.current_price - position.avg_price) / position.avg_price) * 100
            
            total_profit_loss += profit_loss
            
            position_info = {
                'stock_code': stock_code,
                'stock_name': position.stock_name,
                'quantity': position.quantity,
                'avg_price': position.avg_price,
                'current_price': position.current_price,
                'profit_loss': profit_loss,
                'profit_loss_pct': profit_loss_pct,
                'stop_loss_price': position.stop_loss_price,
                'take_profit_price': position.take_profit_price
            }
            
            summary['positions'].append(position_info)
        
        summary['total_profit_loss'] = total_profit_loss
        return summary
    
    async def remove_position(self, stock_code: str):
        """포지션 제거"""
        if stock_code in self.positions:
            position = self.positions[stock_code]
            del self.positions[stock_code]
            self.logger.info(f"📤 포지션 제거: {position.stock_name}({stock_code})")