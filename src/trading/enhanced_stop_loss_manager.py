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
    
class EnhancedStopLossManager:
    """강화된 손절 관리자"""
    
    def __init__(self, api_client, config: Dict = None):
        self.api_client = api_client
        self.config = config or {}
        self.positions: Dict[str, PositionInfo] = {}
        self.logger = logging.getLogger(__name__)
        
        # 손절 설정
        stop_loss_config = self.config.get('enhanced_stop_loss', {})
        self.stop_loss_pct = stop_loss_config.get('stop_loss_pct', 1.5)  # 1.5%로 더 엄격
        self.take_profit_pct = stop_loss_config.get('take_profit_pct', 3.0)
        self.trailing_stop_pct = stop_loss_config.get('trailing_stop_pct', 1.0)
        
        # 즉시 실행 설정
        self.force_execution = stop_loss_config.get('force_execution', True)
        self.max_execution_delay = stop_loss_config.get('max_execution_delay_seconds', 5)
        
        # 실행 큐
        self.execution_queue = asyncio.Queue()
        self.execution_lock = asyncio.Lock()
        self.is_running = False
        
        self.logger.info(f"Enhanced StopLoss Manager 초기화:")
        self.logger.info(f"  • 손절: {self.stop_loss_pct}%")
        self.logger.info(f"  • 익절: {self.take_profit_pct}%") 
        self.logger.info(f"  • 트레일링: {self.trailing_stop_pct}%")
        self.logger.info(f"  • 강제실행: {self.force_execution}")
    
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
            
        # 손절/익절가 계산
        stop_loss_price = avg_price * (1 - self.stop_loss_pct / 100)
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
        self.logger.info(f"   손절가: {stop_loss_price:,.0f}원 ({self.stop_loss_pct}%)")
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
            # 트레일링 스톱 가격 업데이트
            if current_price > position.avg_price * 1.02:  # 2% 이상 상승시 트레일링 활성화
                position.trailing_stop_price = current_price * (1 - self.trailing_stop_pct / 100)
        
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