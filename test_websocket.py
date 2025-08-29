#!/usr/bin/env python3

import asyncio
import os
import logging
from dotenv import load_dotenv
from api_client import KISAPIClient
from utils import setup_logging

async def test_websocket_connection():
    """WebSocket 연결 및 데이터 수신 테스트"""
    load_dotenv()
    
    # DEBUG 로그 레벨로 설정
    setup_logging("DEBUG")
    logger = logging.getLogger(__name__)
    
    # API 클라이언트 초기화
    api_client = KISAPIClient(
        app_key=os.getenv('KIS_APP_KEY'),
        app_secret=os.getenv('KIS_APP_SECRET'),
        account_no=os.getenv('KIS_ACCOUNT_NO'),
        is_demo=True
    )
    
    message_count = 0
    max_messages = 10  # 최대 10개 메시지만 받고 종료
    
    async def message_handler(data):
        nonlocal message_count
        message_count += 1
        
        logger.info(f"=== WebSocket Message #{message_count} ===")
        logger.info(f"Data type: {type(data)}")
        
        if isinstance(data, dict):
            logger.info(f"Keys: {list(data.keys())}")
            
            # 주요 데이터 필드 확인
            stock_code = data.get('mksc_shrn_iscd', 'N/A')
            price = data.get('stck_prpr', 'N/A')
            volume = data.get('cntg_vol', 'N/A')
            
            logger.info(f"Stock Code: {stock_code}")
            logger.info(f"Current Price: {price}")
            logger.info(f"Volume: {volume}")
            
            # 전체 데이터 구조 로그 (처음 3개 메시지만)
            if message_count <= 3:
                logger.debug(f"Full data structure: {data}")
        else:
            logger.info(f"Received non-dict data: {data}")
        
        # 10개 메시지 받으면 종료
        if message_count >= max_messages:
            logger.info(f"Received {max_messages} messages, stopping test")
            return False  # 종료 신호
        
        return True
    
    try:
        async with api_client:
            logger.info("Starting WebSocket test...")
            logger.info("1. Getting access token...")
            
            # WebSocket 연결 (approval key 자동 획득)
            logger.info("2. Getting WebSocket approval key and connecting...")
            await api_client.connect_websocket()
            
            # 실시간 현재가 구독 (삼성전자만 테스트)
            test_stocks = ["005930"]  # 삼성전자
            logger.info(f"3. Subscribing to stocks: {test_stocks}")
            await api_client.subscribe_realtime_price(test_stocks)
            
            # 메시지 수신 (최대 30초간 대기)
            logger.info("4. Listening for messages...")
            
            try:
                await asyncio.wait_for(
                    api_client.listen_websocket(message_handler),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.info("Test completed: 30 second timeout reached")
            
            logger.info(f"Test completed: Received {message_count} messages")
            
            if message_count == 0:
                logger.warning("⚠️  No messages received. Check:")
                logger.warning("  - API credentials are correct")
                logger.warning("  - WebSocket URL is accessible")
                logger.warning("  - Subscription format is correct")
            else:
                logger.info(f"✅ Successfully received {message_count} WebSocket messages!")
                
    except Exception as e:
        logger.error(f"WebSocket test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("WebSocket Connection Test")
    print("=" * 40)
    asyncio.run(test_websocket_connection())