"""
동적 종목 선정 기능 테스트
"""
import asyncio
import os
import logging
from dotenv import load_dotenv
from src.api import KISAPIClient
from src.analysis import DynamicStockSelector

async def test_dynamic_stock_selection():
    """동적 종목 선정 테스트"""
    load_dotenv()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # API 클라이언트 초기화
    api_client = KISAPIClient(
        app_key=os.getenv('KIS_APP_KEY'),
        app_secret=os.getenv('KIS_APP_SECRET'),
        account_no=os.getenv('KIS_ACCOUNT_NO'),
        is_demo=True
    )
    
    try:
        async with api_client:
            logger.info("=== API 연결 테스트 ===")
            
            # 1. 거래량 순위 직접 테스트
            logger.info("1. 거래량 순위 조회 테스트...")
            volume_data = await api_client.get_volume_ranking()
            logger.info(f"Volume ranking response: {volume_data}")
            
            if volume_data and volume_data.get('rt_cd') == '0':
                output = volume_data.get('output', [])
                logger.info(f"Found {len(output)} stocks in volume ranking")
                
                for i, stock in enumerate(output[:5]):
                    logger.info(f"  {i+1}. {stock.get('hts_kor_isnm')}({stock.get('mksc_shrn_iscd')}): "
                              f"가격 {stock.get('stck_prpr')}원, "
                              f"거래량 {stock.get('acml_vol')}주, "
                              f"등락률 {stock.get('prdy_ctrt')}%")
            else:
                logger.error(f"Volume ranking failed: {volume_data}")
            
            # 2. 활발한 종목 조회 테스트
            logger.info("\n2. 활발한 종목 조회 테스트...")
            active_stocks = await api_client.get_active_stocks()
            logger.info(f"Active stocks response: {active_stocks}")
            
            if active_stocks:
                logger.info(f"Found {len(active_stocks)} active stocks")
                for stock in active_stocks:
                    logger.info(f"  - {stock['stock_name']}({stock['stock_code']}): "
                              f"점수 {stock['score']:,.0f}")
            else:
                logger.warning("No active stocks found")
            
            # 3. 동적 종목 선정 시스템 테스트
            logger.info("\n3. 동적 종목 선정 시스템 테스트...")
            selector = DynamicStockSelector(api_client)
            
            # 필터링 기준 확인
            logger.info(f"Current filters: {selector.filters}")
            
            # 종목 선정 실행
            target_stocks = await selector.get_dynamic_target_stocks()
            logger.info(f"Selected target stocks: {target_stocks}")
            
            # 종목 요약 정보
            summary = await selector.get_stock_summary()
            logger.info(f"Stock summary:\n{summary}")
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_dynamic_stock_selection())