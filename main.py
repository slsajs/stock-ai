import asyncio
import os
import logging
from dotenv import load_dotenv
from typing import List
from src.api import KISAPIClient
from src.trading import AutoTrader
from src.utils import TradingConfig, setup_logging, create_trades_csv_if_not_exists, send_telegram_message

async def main():
    # 환경 변수 로드
    load_dotenv()
    
    # 로깅 설정 (DEBUG 레벨로 WebSocket 데이터 확인 가능)
    log_level = os.getenv("LOG_LEVEL", "INFO")
    setup_logging(log_level)
    logger = logging.getLogger(__name__)
    
    # 필수 환경 변수 확인
    required_env_vars = ['KIS_APP_KEY', 'KIS_APP_SECRET', 'KIS_ACCOUNT_NO']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.error("Please copy .env.example to .env and fill in your API credentials")
        return
    
    # 설정 로드
    config = TradingConfig.from_env()
    
    # CSV 파일 초기화
    create_trades_csv_if_not_exists()
    
    # API 클라이언트 초기화
    api_client = KISAPIClient(
        app_key=os.getenv('KIS_APP_KEY'),
        app_secret=os.getenv('KIS_APP_SECRET'),
        account_no=os.getenv('KIS_ACCOUNT_NO'),
        is_demo=True  # 모의투자
    )
    
    # 자동매매 시스템 초기화
    trader = AutoTrader(config, api_client)
    
    try:
        async with api_client:
            logger.info("KIS API Stock Trading System Started")
            await send_telegram_message("주식 자동매매 시스템이 시작되었습니다.", config)
            
            # WebSocket 연결 및 실시간 데이터 수신을 위한 태스크들
            tasks = []
            
            # 메인 트레이딩 루프
            trading_task = asyncio.create_task(trader.start_trading())
            tasks.append(trading_task)
            
            # WebSocket 연결 및 실시간 데이터 수신
            websocket_task = asyncio.create_task(setup_websocket(api_client, trader, config))
            tasks.append(websocket_task)
            
            # 태스크들을 동시에 실행
            await asyncio.gather(*tasks)
            
    except KeyboardInterrupt:
        logger.info("System stopped by user")
        await send_telegram_message("주식 자동매매 시스템이 사용자에 의해 중지되었습니다.", config)
    except Exception as e:
        logger.error(f"System error: {e}")
        await send_telegram_message(f"시스템 오류가 발생했습니다: {e}", config)

async def setup_websocket(api_client: KISAPIClient, trader: AutoTrader, config: TradingConfig):
    """WebSocket 설정 및 실시간 데이터 처리"""
    logger = logging.getLogger(__name__)
    current_subscribed_stocks = []
    
    try:
        # WebSocket 연결 (approval key 자동 획득)
        await api_client.connect_websocket()
        
        # 초기 구독
        await api_client.subscribe_realtime_price(trader.target_stocks)
        current_subscribed_stocks = trader.target_stocks.copy()
        logger.info(f"📡 Initial WebSocket subscription: {current_subscribed_stocks}")
        
        # 실시간 데이터 수신 및 처리 (동적 재구독 포함)
        await listen_with_resubscription(api_client, trader, current_subscribed_stocks)
        
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await send_telegram_message(f"WebSocket 연결 오류: {e}", config)
        
        # 재연결 시도
        await asyncio.sleep(5)
        await setup_websocket(api_client, trader, config)

async def listen_with_resubscription(api_client: KISAPIClient, trader: AutoTrader, current_subscribed_stocks: List[str]):
    """WebSocket 리스닝 + 동적 재구독"""
    logger = logging.getLogger(__name__)
    
    async def data_callback(data):
        # 데이터 처리
        await trader.process_realtime_data(data)
        
        # 재구독 체크
        if getattr(trader, 'need_resubscribe', False):
            if set(trader.target_stocks) != set(current_subscribed_stocks):
                logger.info(f"🔄 Re-subscribing WebSocket for new targets: {trader.target_stocks}")
                try:
                    await api_client.subscribe_realtime_price(trader.target_stocks)
                    current_subscribed_stocks.clear()
                    current_subscribed_stocks.extend(trader.target_stocks)
                    logger.info(f"✅ WebSocket re-subscription successful")
                except Exception as e:
                    logger.error(f"❌ WebSocket re-subscription failed: {e}")
                
                trader.need_resubscribe = False
    
    # 실시간 데이터 수신
    await api_client.listen_websocket(data_callback)

async def test_api_connection():
    """API 연결 테스트 함수"""
    load_dotenv()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    setup_logging(log_level)
    logger = logging.getLogger(__name__)
    
    api_client = KISAPIClient(
        app_key=os.getenv('KIS_APP_KEY'),
        app_secret=os.getenv('KIS_APP_SECRET'),
        account_no=os.getenv('KIS_ACCOUNT_NO'),
        is_demo=True
    )
    
    try:
        async with api_client:
            logger.info("Testing API connection...")
            
            # 현재가 조회 테스트
            result = await api_client.get_current_price("005930")  # 삼성전자
            if result.get('rt_cd') == '0':
                price = result['output']['stck_prpr']
                logger.info(f"Samsung Electronics current price: {price}")
            else:
                logger.error(f"API test failed: {result}")
            
            # 잔고 조회 테스트
            balance_result = await api_client.get_balance()
            if balance_result.get('rt_cd') == '0':
                logger.info("Balance inquiry successful")
            else:
                logger.error(f"Balance inquiry failed: {balance_result}")
                
    except Exception as e:
        logger.error(f"API connection test failed: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # 테스트 모드
        asyncio.run(test_api_connection())
    else:
        # 메인 실행
        asyncio.run(main())