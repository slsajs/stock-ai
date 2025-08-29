#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import calculate_rsi, calculate_sma, calculate_ema, TradingConfig

def test_rsi_calculation():
    """RSI 계산 테스트"""
    print("=== RSI 계산 테스트 ===")
    
    # 테스트 데이터: 상승 추세
    rising_prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 109, 111, 110, 112, 114, 115]
    rsi = calculate_rsi(rising_prices)
    print(f"상승 추세 RSI: {rsi:.2f} (70 이상이면 과매수)")
    
    # 테스트 데이터: 하락 추세
    falling_prices = [115, 113, 114, 112, 110, 111, 109, 107, 108, 106, 104, 105, 103, 101, 100]
    rsi = calculate_rsi(falling_prices)
    print(f"하락 추세 RSI: {rsi:.2f} (30 이하면 과매도)")
    
    # 짧은 데이터 (RSI 계산 불가)
    short_prices = [100, 102, 101]
    rsi = calculate_rsi(short_prices)
    print(f"짧은 데이터 RSI: {rsi} (None이어야 함)")
    print()

def test_moving_averages():
    """이동평균 계산 테스트"""
    print("=== 이동평균 계산 테스트 ===")
    
    prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 109]
    
    sma_5 = calculate_sma(prices, 5)
    print(f"5일 단순이동평균: {sma_5:.2f}")
    
    ema_5 = calculate_ema(prices, 5)
    print(f"5일 지수이동평균: {ema_5:.2f}")
    
    # 데이터 부족 테스트
    short_prices = [100, 102]
    sma_5_short = calculate_sma(short_prices, 5)
    print(f"데이터 부족시 SMA: {sma_5_short} (None이어야 함)")
    print()

def test_trading_config():
    """TradingConfig 테스트"""
    print("=== TradingConfig 테스트 ===")
    
    # 환경변수에서 로드
    config = TradingConfig.from_env()
    print(f"최대 포지션: {config.max_positions}")
    print(f"손절 비율: {config.stop_loss_pct}%")
    print(f"익절 비율: {config.take_profit_pct}%")
    print(f"RSI 과매도: {config.rsi_oversold}")
    print(f"RSI 과매수: {config.rsi_overbought}")
    print(f"거래량 배수: {config.volume_multiplier}")
    print()

def test_position_logic():
    """포지션 로직 테스트"""
    print("=== 포지션 로직 테스트 ===")
    
    from trader import Position
    from datetime import datetime
    
    # 수익 포지션
    profit_position = Position(
        stock_code="005930",
        quantity=10,
        avg_price=100000,
        purchase_time=datetime.now(),
        current_price=105000
    )
    
    print(f"종목: {profit_position.stock_code}")
    print(f"수량: {profit_position.quantity}주")
    print(f"평균단가: {profit_position.avg_price:,}원")
    print(f"현재가: {profit_position.current_price:,}원")
    print(f"손익률: {profit_position.profit_loss_pct:.2f}%")
    print(f"손익금액: {profit_position.profit_loss_amount:,}원")
    
    # 손실 포지션
    loss_position = Position(
        stock_code="000660",
        quantity=5,
        avg_price=100000,
        purchase_time=datetime.now(),
        current_price=95000
    )
    
    print(f"\n종목: {loss_position.stock_code}")
    print(f"손익률: {loss_position.profit_loss_pct:.2f}%")
    print(f"손익금액: {loss_position.profit_loss_amount:,}원")
    print()

def test_csv_logging():
    """CSV 로깅 테스트"""
    print("=== CSV 로깅 테스트 ===")
    
    from utils import create_trades_csv_if_not_exists
    
    # CSV 파일 생성 테스트
    create_trades_csv_if_not_exists()
    print("trades.csv 파일 생성 완료")
    
    # 파일 존재 확인
    if os.path.exists('trades.csv'):
        print("trades.csv 파일이 성공적으로 생성되었습니다")
        
        with open('trades.csv', 'r', encoding='utf-8') as f:
            header = f.readline().strip()
            print(f"CSV 헤더: {header}")
    else:
        print("trades.csv 파일 생성 실패")
    print()

def main():
    print("KIS 주식 자동매매 시스템 - 기능 테스트")
    print("=" * 50)
    
    try:
        test_trading_config()
        test_rsi_calculation()
        test_moving_averages()
        test_position_logic()
        test_csv_logging()
        
        print("모든 기본 기능 테스트 완료!")
        print()
        print("다음 단계:")
        print("1. 실제 KIS API 키를 .env 파일에 설정")
        print("2. python main.py test 로 API 연결 테스트")
        print("3. python main.py 로 자동매매 실행")
        
    except Exception as e:
        print(f"테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()