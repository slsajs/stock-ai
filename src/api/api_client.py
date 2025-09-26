import asyncio
import aiohttp
import websockets
import json
import logging
from datetime import datetime
from typing import Dict, Optional, List
import hashlib
import hmac
import base64
from urllib.parse import urlencode
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

from ..cache.financial_data_manager import FinancialDataManager

logger = logging.getLogger(__name__)

class KISAPIClient:
    def __init__(self, app_key: str, app_secret: str, account_no: str, is_demo: bool = True):
        self.app_key = app_key
        self.app_secret = app_secret
        self.account_no = account_no
        self.is_demo = is_demo
        
        self.base_url = "https://openapivts.koreainvestment.com:29443" if is_demo else "https://openapi.koreainvestment.com:9443"
        self.ws_url = "ws://ops.koreainvestment.com:21000" if is_demo else "ws://ops.koreainvestment.com:31000"
        
        self.access_token = None
        self.session = None
        self.websocket = None
        self.encryption_key = None
        self.encryption_iv = None

        self.rate_limiter = asyncio.Semaphore(20)  # 초당 20회 제한

        # WebSocket 연결 안정성 관련 변수
        self.ws_reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5  # 초
        self.is_reconnecting = False
        self.last_heartbeat = None
        self.heartbeat_interval = 30  # 30초마다 heartbeat

        # 캐싱 및 폴백 로직을 위한 데이터 매니저
        self.data_manager = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        await self.get_access_token()

        # 데이터 매니저 초기화
        self.data_manager = FinancialDataManager(self)

        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # WebSocket 정리
        await self.close_websocket()

        # HTTP 세션 정리
        if self.session:
            await self.session.close()
    
    async def get_access_token(self):
        """액세스 토큰 획득"""
        url = f"{self.base_url}/oauth2/tokenP"
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        async with self.session.post(url, json=data) as response:
            if response.status == 200:
                result = await response.json()
                self.access_token = result.get("access_token")
                logger.info("Access token obtained successfully")
            else:
                logger.error(f"Failed to get access token: {response.status}")
                raise Exception("Failed to get access token")
    
    def _parse_account_no(self):
        """계좌번호를 안전하게 파싱"""
        try:
            if "-" in self.account_no:
                parts = self.account_no.split("-")
                if len(parts) >= 2:
                    return parts[0], parts[1]
            
            # 하이픈이 없는 경우 (예: 12345678901234)
            if len(self.account_no) >= 10:
                # 보통 앞 8자리가 계좌번호, 뒤 2자리가 상품코드
                cano = self.account_no[:-2]
                acnt_prdt_cd = self.account_no[-2:]
                logger.info(f"Parsed account: {cano}-{acnt_prdt_cd}")
                return cano, acnt_prdt_cd
            
            # 기본값 반환
            logger.warning(f"Unable to parse account_no: {self.account_no}, using as-is")
            return self.account_no, "01"
            
        except Exception as e:
            logger.error(f"Error parsing account_no {self.account_no}: {e}")
            return self.account_no, "01"

    def _get_headers(self, tr_id: str, custtype: str = "P"):
        """API 요청 헤더 생성"""
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": custtype
        }
    
    async def _request(self, method: str, url: str, headers: Dict, data: Optional[Dict] = None):
        """API 요청 (Rate Limiting 적용)"""
        async with self.rate_limiter:
            if method.upper() == "GET":
                async with self.session.get(url, headers=headers, params=data) as response:
                    return await response.json()
            elif method.upper() == "POST":
                async with self.session.post(url, headers=headers, json=data) as response:
                    return await response.json()
    
    async def get_current_price(self, stock_code: str) -> Dict:
        """현재가 조회 (스로틀링 적용)"""
        # API 호출 제한 적용
        from ..utils.api_throttler import throttler
        await throttler.throttle()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._get_headers("FHKST01010100")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code
        }
        
        return await self._request("GET", url, headers, params)
    
    async def get_orderbook(self, stock_code: str) -> Dict:
        """호가 정보 조회"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
        headers = self._get_headers("FHKST01010200")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code
        }
        
        return await self._request("GET", url, headers, params)
    
    async def place_order(self, stock_code: str, order_type: str, quantity: int, price: int = 0) -> Dict:
        """주문 실행 (스로틀링 적용)"""
        # API 호출 제한 적용
        from ..utils.api_throttler import throttler
        await throttler.throttle()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash"
        
        tr_id = "VTTC0802U" if order_type == "buy" else "VTTC0801U"  # 모의투자
        if not self.is_demo:
            tr_id = "TTTC0802U" if order_type == "buy" else "TTTC0801U"  # 실투자
            
        headers = self._get_headers(tr_id)
        
        # 계좌번호 안전하게 파싱
        cano, acnt_prdt_cd = self._parse_account_no()
        
        data = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "PDNO": stock_code,
            "ORD_DVSN": "01" if price > 0 else "01",  # 01: 지정가, 01: 시장가
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price) if price > 0 else "0"
        }
        
        return await self._request("POST", url, headers, data)
    
    async def get_balance(self) -> Dict:
        """잔고 조회 (스로틀링 적용)"""
        # API 호출 제한 적용
        from ..utils.api_throttler import throttler
        await throttler.throttle()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
        headers = self._get_headers("VTTC8434R" if self.is_demo else "TTTC8434R")
        
        # 계좌번호 안전하게 파싱
        cano, acnt_prdt_cd = self._parse_account_no()
        
        params = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
        
        return await self._request("GET", url, headers, params)
    
    async def get_minute_data(self, stock_code: str, period: str = "1") -> Dict:
        """분봉 데이터 조회"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        headers = self._get_headers("FHKST03010200")
        params = {
            "fid_etc_cls_code": "",
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code,
            "fid_input_hour_1": period,
            "fid_pw_data_incu_yn": "Y"
        }
        
        return await self._request("GET", url, headers, params)
    
    async def get_volume_ranking(self, market: str = "J", sort: str = "1", count: int = 30) -> Dict:
        """거래량 순위 조회 (스로틀링 적용)"""
        try:
            # API 호출 제한 적용
            from ..utils.api_throttler import throttler
            await throttler.throttle()
            
            url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/volume-rank"
            headers = self._get_headers("FHPST01710000")
            params = {
                "fid_cond_mrkt_div_code": market,  # J: 코스피+코스닥, 0: 코스피, 1: 코스닥
                "fid_cond_scr_div_code": "20171",
                "fid_input_iscd": "0000",
                "fid_div_cls_code": "0",
                "fid_blng_cls_code": "0",
                "fid_trgt_cls_code": "111111111",
                "fid_trgt_exls_cls_code": "000000",
                "fid_input_price_1": "",
                "fid_input_price_2": "",
                "fid_vol_cnt": "",
                "fid_input_date_1": ""
            }
            
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Volume ranking request: URL={url}")
            result = await self._request("GET", url, headers, params)
            
            if result.get('rt_cd') == '1':
                logger.warning(f"Volume ranking API 실패: {result.get('msg1', 'Unknown error')}")
                
            return result
        except Exception as e:
            import logging
            import traceback
            logger = logging.getLogger(__name__)
            logger.error(f"Volume ranking API error: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"rt_cd": "1", "msg1": f"API Error: {e}"}
    
    async def get_daily_price(self, stock_code: str, start_date: str, end_date: str) -> Dict:
        """일봉 데이터 조회 (스로틀링 적용)"""
        # API 호출 제한 적용
        from ..utils.api_throttler import throttler
        await throttler.throttle()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        headers = self._get_headers("FHKST03010100")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code,
            "fid_input_date_1": start_date,
            "fid_input_date_2": end_date,
            "fid_period_div_code": "D",  # 일봉
            "fid_org_adj_prc": "1"       # 수정주가
        }
        
        return await self._request("GET", url, headers, params)
    
    async def get_index(self, index_code: str) -> Dict:
        """지수 조회 (스로틀링 적용)"""
        # API 호출 제한 적용
        from ..utils.api_throttler import throttler
        await throttler.throttle()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-index-price"
        headers = self._get_headers("FHKUP03500100")  # 지수시세 조회 API 코드로 변경
        params = {
            "fid_cond_mrkt_div_code": "U",
            "fid_input_iscd": index_code
        }
        
        return await self._request("GET", url, headers, params)
    
    async def get_market_cap_ranking(self, market: str = "J", count: int = 30) -> Dict:
        """시가총액 순위 조회"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        headers = self._get_headers("FHKST03010100")
        params = {
            "fid_cond_mrkt_div_code": market,
            "fid_input_iscd": "0001",  # 시가총액 상위
            "fid_input_date_1": datetime.now().strftime("%Y%m%d"),
            "fid_input_date_2": datetime.now().strftime("%Y%m%d"),
            "fid_period_div_code": "D"
        }
        
        return await self._request("GET", url, headers, params)
    
    async def get_fluctuation_ranking(self, market: str = "J", sort_type: str = "1") -> Dict:
        """등락률 순위 조회"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        headers = self._get_headers("FHPST01730000")
        params = {
            "fid_cond_mrkt_div_code": market,
            "fid_cond_scr_div_code": "20173",
            "fid_input_iscd": "0001",
            "fid_rank_sort_cls_code": sort_type,  # 1: 상승률, 2: 하락률
            "fid_input_cnt_1": "30",
            "fid_prc_cls_code": "1",
            "fid_input_price_1": "1000",  # 최소가격 1000원
            "fid_input_price_2": "100000"  # 최대가격 100000원
        }
        
        return await self._request("GET", url, headers, params)
    
    async def get_active_stocks(self, min_price: int = 5000, max_price: int = 100000, volume_data: Dict = None) -> List[Dict]:
        """활발한 거래 종목 조회 (거래량 + 등락률 조합)"""
        try:
            # 이미 volume_data가 있으면 재사용, 없으면 새로 조회
            if not volume_data:
                await asyncio.sleep(1)  # Rate limiting
                volume_data = await self.get_volume_ranking()
            
            if not volume_data or volume_data.get('rt_cd') != '0':
                logger.error("Failed to get volume ranking for active stocks")
                return []
            
            active_stocks = []
            for item in volume_data.get('output', [])[:30]:  # 상위 30개 검토
                try:
                    stock_code = item.get('mksc_shrn_iscd', '')
                    stock_name = item.get('hts_kor_isnm', '')
                    current_price = float(item.get('stck_prpr', 0))
                    volume = int(item.get('acml_vol', 0))
                    change_rate = float(item.get('prdy_ctrt', 0))
                    
                    # ETF, ETN 제외
                    if any(exclude in stock_name for exclude in ['ETF', 'ETN', 'KODEX', 'TIGER', 'KBSTAR']):
                        continue
                    
                    # 관리종목 제외 (종목코드로 판단)
                    if not stock_code or len(stock_code) != 6 or stock_code.startswith(('9', 'Q')):
                        continue
                    
                    # 너무 극단적인 등락률 제외
                    if abs(change_rate) > 30.0:
                        continue
                    
                    # 필터링 조건
                    if (min_price <= current_price <= max_price and 
                        volume > 1000000 and  # 거래량 100만주 이상
                        abs(change_rate) > 1.0):  # 등락률 1% 이상
                        
                        # 점수 계산 (거래량 * 등락률 * 가격 보정)
                        price_factor = 1.0
                        if current_price < 10000:
                            price_factor = 0.8  # 저가주 감점
                        elif current_price > 50000:
                            price_factor = 0.9  # 고가주 감점
                        
                        score = volume * abs(change_rate) * price_factor
                        
                        active_stocks.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'volume': volume,
                            'change_rate': change_rate,
                            'score': score
                        })
                        
                except (ValueError, TypeError) as e:
                    logger.debug(f"Error parsing stock data: {e}")
                    continue
            
            # 점수 순으로 정렬
            active_stocks.sort(key=lambda x: x['score'], reverse=True)
            logger.info(f"Found {len(active_stocks)} active stocks after filtering")
            
            return active_stocks[:10]  # 상위 10개만 반환
            
        except Exception as e:
            logger.error(f"Error getting active stocks: {e}")
            return []
    
    async def get_websocket_approval_key(self):
        """WebSocket 접속키 발급"""
        url = f"{self.base_url}/oauth2/Approval"
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret
        }
        
        async with self.session.post(url, json=data) as response:
            if response.status == 200:
                result = await response.json()
                approval_key = result.get("approval_key")
                logger.info("WebSocket approval key obtained successfully")
                logger.debug(f"Approval key: {approval_key}")
                return approval_key
            else:
                logger.error(f"Failed to get WebSocket approval key: {response.status}")
                text = await response.text()
                logger.debug(f"Response: {text}")
                return None
    
    async def connect_websocket(self, approval_key: str = None):
        """WebSocket 연결 (자동 재연결 지원)"""
        try:
            if not approval_key:
                approval_key = await self.get_websocket_approval_key()
                if not approval_key:
                    raise Exception("Failed to get WebSocket approval key")

            self.approval_key = approval_key
            logger.debug(f"Using approval key: {approval_key}")
            logger.debug(f"Attempting WebSocket connection to: {self.ws_url}")

            # WebSocket 연결 옵션 설정
            extra_headers = {
                "User-Agent": "Python-KIS-API/1.0"
            }

            # ping/pong 설정으로 연결 유지
            self.websocket = await websockets.connect(
                self.ws_url,
                extra_headers=extra_headers,
                ping_interval=20,  # 20초마다 ping
                ping_timeout=10,   # ping 응답 대기시간 10초
                close_timeout=10   # 연결 종료 대기시간 10초
            )

            logger.info("WebSocket connected successfully")
            logger.debug(f"WebSocket state: {self.websocket.state}")

            # 연결 성공 시 재연결 카운터 리셋
            self.ws_reconnect_attempts = 0
            self.is_reconnecting = False
            self.last_heartbeat = datetime.now()

        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            logger.debug(f"WebSocket URL: {self.ws_url}")
            raise

    async def _reconnect_websocket(self):
        """WebSocket 자동 재연결"""
        if self.is_reconnecting:
            return

        self.is_reconnecting = True

        while self.ws_reconnect_attempts < self.max_reconnect_attempts:
            try:
                self.ws_reconnect_attempts += 1
                wait_time = min(self.reconnect_delay * self.ws_reconnect_attempts, 60)

                logger.warning(f"WebSocket reconnection attempt {self.ws_reconnect_attempts}/{self.max_reconnect_attempts} in {wait_time}s")
                await asyncio.sleep(wait_time)

                # 기존 연결 정리
                if self.websocket:
                    try:
                        await self.websocket.close()
                    except:
                        pass
                    self.websocket = None

                # 재연결 시도
                await self.connect_websocket()
                logger.info("WebSocket reconnected successfully")
                return

            except Exception as e:
                logger.error(f"WebSocket reconnection attempt {self.ws_reconnect_attempts} failed: {e}")
                if self.ws_reconnect_attempts >= self.max_reconnect_attempts:
                    logger.error("Maximum reconnection attempts reached. WebSocket connection abandoned.")
                    break

        self.is_reconnecting = False

    def is_websocket_connected(self) -> bool:
        """WebSocket 연결 상태 확인"""
        return (
            self.websocket is not None and
            not self.websocket.closed and
            self.websocket.state == websockets.protocol.State.OPEN
        )
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """WebSocket 데이터 복호화"""
        if not CRYPTO_AVAILABLE:
            logger.warning("pycryptodome not available, cannot decrypt data")
            return encrypted_data
            
        if not self.encryption_key or not self.encryption_iv:
            logger.warning("Encryption key/iv not available, cannot decrypt data")
            return encrypted_data
            
        try:
            # Base64 디코딩
            encrypted_bytes = base64.b64decode(encrypted_data)
            
            # AES CBC 복호화
            cipher = AES.new(
                self.encryption_key.encode('utf-8')[:32].ljust(32, b'\0'), 
                AES.MODE_CBC, 
                self.encryption_iv.encode('utf-8')[:16].ljust(16, b'\0')
            )
            
            decrypted = cipher.decrypt(encrypted_bytes)
            decrypted_data = unpad(decrypted, AES.block_size).decode('utf-8')
            
            logger.debug(f"Successfully decrypted data: {decrypted_data[:100]}...")
            return decrypted_data
            
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            return encrypted_data
    
    async def subscribe_realtime_price(self, stock_codes: List[str]):
        """실시간 현재가 구독 (연결 상태 확인 포함)"""
        if not self.is_websocket_connected():
            logger.warning("WebSocket not connected, attempting reconnection...")
            await self._reconnect_websocket()

        if not self.is_websocket_connected():
            raise Exception("WebSocket connection failed after reconnection attempts")

        logger.debug(f"Subscribing to {len(stock_codes)} stock codes: {stock_codes}")

        for stock_code in stock_codes:
            try:
                subscribe_data = {
                    "header": {
                        "approval_key": getattr(self, 'approval_key', self.app_key),
                        "custtype": "P",
                        "tr_type": "1",
                        "content-type": "utf-8"
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STCNT0",
                            "tr_key": stock_code
                        }
                    }
                }

                logger.debug(f"Sending subscription data for {stock_code}: {json.dumps(subscribe_data, indent=2)}")
                await self.websocket.send(json.dumps(subscribe_data))
                logger.info(f"Subscribed to real-time price for {stock_code}")

                # 구독 간 짧은 대기
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Failed to subscribe to {stock_code}: {e}")
                # 연결 문제가 의심되면 재연결 시도
                if "connection" in str(e).lower() or "closed" in str(e).lower():
                    logger.warning("Connection issue detected, attempting reconnection...")
                    await self._reconnect_websocket()
                raise
    
    def _parse_realtime_data(self, data_str: str) -> Dict:
        """실시간 파이프 구분 데이터 파싱"""
        try:
            parts = data_str.split('|')
            if len(parts) < 4:
                return None
                
            compress_flag = parts[0]
            tr_id = parts[1]
            seq = parts[2]
            data_part = parts[3]
            
            # H0STCNT0 (주식 현재가) 데이터 파싱
            if tr_id == "H0STCNT0":
                fields = data_part.split('^')
                if len(fields) >= 15:
                    def safe_int(value, default=0):
                        try:
                            return int(value) if value and value != '' else default
                        except ValueError:
                            return default
                    
                    def safe_float(value, default=0.0):
                        try:
                            return float(value) if value and value != '' else default
                        except ValueError:
                            return default
                    
                    return {
                        "tr_id": tr_id,
                        "stock_code": fields[0],
                        "time": fields[1],
                        "current_price": safe_int(fields[2]),
                        "change": safe_int(fields[4]),
                        "change_rate": safe_float(fields[5]),
                        "volume": safe_int(fields[12]),
                        "trade_value": safe_int(fields[13]),
                        "bid_price": safe_int(fields[7]),
                        "ask_price": safe_int(fields[8]),
                        "high_price": safe_int(fields[9]),
                        "low_price": safe_int(fields[10]),
                        "prev_close": safe_int(fields[11])
                    }
            
            # 다른 TR_ID도 필요시 추가
            return {
                "tr_id": tr_id,
                "raw_data": data_part,
                "compress_flag": compress_flag,
                "seq": seq
            }
            
        except Exception as e:
            logger.error(f"Failed to parse realtime data: {e}")
            return None

    async def listen_websocket(self, callback):
        """WebSocket 메시지 수신 (연결 모니터링 및 자동 재연결 포함)"""
        if not self.is_websocket_connected():
            raise Exception("WebSocket not connected")

        logger.info("Starting WebSocket message listener with auto-reconnection")
        message_count = 0
        last_message_time = datetime.now()

        while True:
            try:
                # 연결 상태 확인
                if not self.is_websocket_connected():
                    logger.warning("WebSocket connection lost, attempting reconnection...")
                    await self._reconnect_websocket()
                    if not self.is_websocket_connected():
                        logger.error("Failed to reconnect WebSocket")
                        break

                # Heartbeat 체크
                await self._check_heartbeat()

                # 메시지 수신 (타임아웃 설정)
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=30.0)
                    message_count += 1
                    last_message_time = datetime.now()

                    logger.debug(f"Received WebSocket message #{message_count}")
                    logger.debug(f"Message type: {type(message)}, length: {len(message) if message else 0}")

                    if message:
                        logger.debug(f"Raw message (first 200 chars): {str(message)[:200]}...")

                    # 빈 메시지나 ping/pong 메시지 무시
                    if not message or message in ['ping', 'pong']:
                        logger.debug(f"Ignoring empty or ping/pong message #{message_count}")
                        continue

                    # 여러 JSON 객체가 연결된 경우 처리
                    message_str = str(message).strip()
                    if message_str.count('{') > 1:
                        logger.debug(f"Message contains multiple JSON objects, processing first one")
                        # 첫 번째 완전한 JSON 객체만 추출
                        brace_count = 0
                        first_json_end = 0
                        for i, char in enumerate(message_str):
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    first_json_end = i + 1
                                    break
                        if first_json_end > 0:
                            message_str = message_str[:first_json_end]

                    # JSON 메시지 처리 시도
                    try:
                        data = json.loads(message_str)
                        logger.debug(f"Parsed JSON data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")

                        # 구독 성공 메시지에서 암호화 키 저장
                        if isinstance(data, dict) and data.get('body', {}).get('msg1') == 'SUBSCRIBE SUCCESS':
                            output = data.get('body', {}).get('output', {})
                            if 'key' in output and 'iv' in output:
                                self.encryption_key = output['key']
                                self.encryption_iv = output['iv']
                                logger.info("Encryption key/iv obtained from subscribe success message")
                                logger.debug(f"Key: {self.encryption_key}, IV: {self.encryption_iv}")
                            # 구독 성공 메시지는 콜백 호출하지 않음
                            continue

                        # 암호화된 실시간 데이터 처리
                        elif isinstance(data, dict) and data.get('header', {}).get('encrypt') == 'Y':
                            logger.debug("Received encrypted real-time data")
                            if 'body' in data and isinstance(data['body'], str):
                                decrypted_body = self.decrypt_data(data['body'])
                                try:
                                    data['body'] = json.loads(decrypted_body)
                                    logger.debug("Successfully decrypted and parsed real-time data")
                                except json.JSONDecodeError:
                                    logger.warning("Failed to parse decrypted data as JSON")

                        # 유의미한 데이터만 콜백 처리
                        if isinstance(data, dict) and ('header' in data or 'body' in data):
                            logger.info(f"Processing JSON WebSocket message #{message_count}")
                            await callback(data)

                    except json.JSONDecodeError:
                        # JSON이 아닌 경우 파이프 구분 데이터 파싱 시도
                        if '|' in message_str:
                            logger.debug(f"Attempting to parse pipe-separated data #{message_count}")
                            parsed_data = self._parse_realtime_data(message_str)
                            if parsed_data:
                                logger.info(f"Processing realtime data for {parsed_data.get('stock_code', 'unknown')}: {parsed_data.get('current_price', 0)}")
                                await callback(parsed_data)
                            else:
                                logger.debug(f"Failed to parse realtime data #{message_count}")
                        else:
                            logger.debug(f"Skipping non-JSON/non-pipe message #{message_count}: {message_str[:50]}...")

                    except Exception as e:
                        logger.error(f"Error processing WebSocket message #{message_count}: {e}")

                except asyncio.TimeoutError:
                    # 30초 동안 메시지가 없으면 연결 상태 의심
                    time_since_last = (datetime.now() - last_message_time).total_seconds()
                    if time_since_last > 60:  # 1분 이상 메시지 없음
                        logger.warning(f"No messages received for {time_since_last:.0f}s, checking connection...")
                        if not self.is_websocket_connected():
                            logger.warning("Connection lost, attempting reconnection...")
                            await self._reconnect_websocket()
                    continue

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
                await self._reconnect_websocket()
                continue

            except Exception as e:
                logger.error(f"WebSocket listener error: {e}")
                logger.debug(f"Total messages processed: {message_count}")

                # 심각한 오류인 경우 재연결 시도
                if "connection" in str(e).lower() or "closed" in str(e).lower():
                    await self._reconnect_websocket()
                    continue
                else:
                    # 다른 오류는 짧은 대기 후 계속
                    await asyncio.sleep(1)
                    continue

        logger.info(f"WebSocket listener stopped. Total messages processed: {message_count}")

    async def _check_heartbeat(self):
        """Heartbeat 상태 확인 및 전송"""
        if not self.last_heartbeat:
            self.last_heartbeat = datetime.now()
            return

        time_since_heartbeat = (datetime.now() - self.last_heartbeat).total_seconds()

        if time_since_heartbeat > self.heartbeat_interval:
            try:
                if self.is_websocket_connected():
                    # 간단한 heartbeat 메시지 전송 (ping 형태)
                    heartbeat_data = {
                        "header": {
                            "approval_key": getattr(self, 'approval_key', self.app_key),
                            "custtype": "P",
                            "tr_type": "1",
                            "content-type": "utf-8"
                        },
                        "body": {
                            "input": {
                                "tr_id": "PINGPONG",
                                "tr_key": "heartbeat"
                            }
                        }
                    }

                    await self.websocket.send(json.dumps(heartbeat_data))
                    self.last_heartbeat = datetime.now()
                    logger.debug("Heartbeat sent to maintain WebSocket connection")

            except Exception as e:
                logger.warning(f"Failed to send heartbeat: {e}")
                # heartbeat 실패 시 연결 상태 의심
                if not self.is_websocket_connected():
                    logger.warning("Heartbeat failed - connection appears to be lost")

    async def close_websocket(self):
        """WebSocket 연결 정리"""
        if self.websocket:
            try:
                logger.info("Closing WebSocket connection...")
                await self.websocket.close()
                logger.info("WebSocket connection closed")
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
            finally:
                self.websocket = None
                self.encryption_key = None
                self.encryption_iv = None
                self.last_heartbeat = None
                self.ws_reconnect_attempts = 0
                self.is_reconnecting = False

    def get_websocket_status(self) -> Dict:
        """WebSocket 연결 상태 정보 반환"""
        status = {
            "connected": self.is_websocket_connected(),
            "reconnect_attempts": self.ws_reconnect_attempts,
            "is_reconnecting": self.is_reconnecting,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "has_encryption_keys": bool(self.encryption_key and self.encryption_iv)
        }

        if self.websocket:
            status["websocket_state"] = str(self.websocket.state)
            status["websocket_closed"] = self.websocket.closed

        return status

    async def get_financial_data(self, stock_code: str) -> Dict:
        """재무정보 조회"""
        # API 호출 제한 적용
        from ..utils.api_throttler import throttler
        await throttler.throttle()
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/finance/balance-sheet"
        headers = self._get_headers("FHKST66430200")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code,
            "fid_input_date_1": "",  # 최근 데이터
        }
        
        return await self._request("GET", url, headers, params)
    
    async def get_stock_overview(self, stock_code: str) -> Dict:
        """종목 개요 및 재무지표 조회 (DEPRECATED - use get_financial_ratios instead)"""
        # API 호출 제한 적용
        from ..utils.api_throttler import throttler
        await throttler.throttle()

        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        headers = self._get_headers("FHKST01010100")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code,
            "fid_org_adj_prc": "0"
        }

        return await self._request("GET", url, headers, params)

    async def get_financial_ratios(self, stock_code: str) -> Dict:
        """재무비율 조회 (PER, PBR, ROE, PSR 등) - 다중 TR_ID 시도"""
        # API 호출 제한 적용
        from ..utils.api_throttler import throttler
        await throttler.throttle()

        # 재무비율 관련 TR_ID 목록 (우선순위 순)
        tr_ids_to_try = [
            ("FHKST03010100", "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"),  # 일봉 데이터 (재무비율 포함 가능)
            ("FHKST01010100", "/uapi/domestic-stock/v1/quotations/inquire-price"),  # 현재가 시세 (일부 재무비율 포함)
            ("FHKST66430100", "/uapi/domestic-stock/v1/finance/balance-sheet"),  # 재무제표
            ("FHKST66430200", "/uapi/domestic-stock/v1/finance/financial-ratio"),  # 원래 시도하던 것
        ]

        last_error = None

        for tr_id, endpoint in tr_ids_to_try:
            try:
                url = f"{self.base_url}{endpoint}"
                headers = self._get_headers(tr_id)

                if tr_id == "FHKST03010100":
                    # 일봉 데이터 파라미터
                    params = {
                        "fid_cond_mrkt_div_code": "J",
                        "fid_input_iscd": stock_code,
                        "fid_input_date_1": datetime.now().strftime("%Y%m%d"),
                        "fid_input_date_2": datetime.now().strftime("%Y%m%d"),
                        "fid_period_div_code": "D",
                        "fid_org_adj_prc": "1"
                    }
                elif tr_id == "FHKST01010100":
                    # 현재가 시세 파라미터
                    params = {
                        "fid_cond_mrkt_div_code": "J",
                        "fid_input_iscd": stock_code
                    }
                else:
                    # 기본 재무 관련 파라미터
                    params = {
                        "fid_cond_mrkt_div_code": "J",
                        "fid_input_iscd": stock_code,
                        "fid_input_date_1": "",
                    }

                result = await self._request("GET", url, headers, params)

                if result and result.get('rt_cd') == '0':
                    logger.debug(f"Financial ratios API success with TR_ID: {tr_id}")
                    return result
                else:
                    logger.debug(f"Financial ratios API failed with TR_ID {tr_id}: {result.get('msg1', 'Unknown error')}")
                    last_error = result

            except Exception as e:
                logger.debug(f"Exception with TR_ID {tr_id}: {e}")
                last_error = {"rt_cd": "1", "msg1": f"Exception: {e}"}
                continue

        # 모든 TR_ID 실패 시 마지막 에러 반환
        logger.warning(f"All financial ratio TR_IDs failed for {stock_code}")
        return last_error or {"rt_cd": "1", "msg1": "All financial ratio APIs failed"}
    
    async def calculate_pbr(self, stock_code: str) -> Optional[float]:
        """PBR 계산 (주가순자산비율) - 새로운 재무지표 API 사용"""
        try:
            # 재무비율 API로 직접 조회
            financial_data = await self.get_financial_ratios(stock_code)
            if financial_data and financial_data.get('rt_cd') == '0':
                output = financial_data.get('output', {})

                # PBR이 직접 제공되는지 확인
                for pbr_key in ['pbr', 'per_pbr', 'stck_pbpr']:
                    pbr_value = output.get(pbr_key)
                    if pbr_value and pbr_value != '0' and pbr_value != '-':
                        try:
                            pbr = float(pbr_value)
                            if 0.01 <= pbr <= 15.0:  # 합리적 범위
                                logger.debug(f"Direct PBR for {stock_code}: {pbr:.2f}")
                                return pbr
                        except (ValueError, TypeError):
                            continue

            # 폴백: 기존 방식으로 계산
            logger.debug(f"Falling back to manual PBR calculation for {stock_code}")
            price_data = await self.get_current_price(stock_code)
            if not price_data or price_data.get('rt_cd') != '0':
                return None

            current_price = float(price_data['output'].get('stck_prpr', 0))
            if current_price <= 0:
                return None

            # BPS 정보 조회 시도
            overview_data = await self.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})
                bps = output.get('bps')
                if bps and float(bps) > 0:
                    pbr = current_price / float(bps)
                    if 0.01 <= pbr <= 15.0:
                        logger.debug(f"Calculated PBR for {stock_code}: {pbr:.2f}")
                        return pbr

            return None

        except Exception as e:
            logger.error(f"Error calculating PBR for {stock_code}: {e}")
            return None
    
    async def calculate_per(self, stock_code: str) -> Optional[float]:
        """PER 계산 (주가수익비율) - 새로운 재무지표 API 사용"""
        try:
            # 재무비율 API로 직접 조회
            financial_data = await self.get_financial_ratios(stock_code)
            if financial_data and financial_data.get('rt_cd') == '0':
                output = financial_data.get('output', {})

                # PER이 직접 제공되는지 확인
                for per_key in ['per', 'stck_per', 'per_ratio']:
                    per_value = output.get(per_key)
                    if per_value and per_value != '0' and per_value != '-':
                        try:
                            per = float(per_value)
                            if 0.1 <= per <= 300.0:  # 합리적 범위
                                logger.debug(f"Direct PER for {stock_code}: {per:.2f}")
                                return per
                        except (ValueError, TypeError):
                            continue

            # 폴백: 기존 방식으로 계산
            logger.debug(f"Falling back to manual PER calculation for {stock_code}")
            price_data = await self.get_current_price(stock_code)
            if not price_data or price_data.get('rt_cd') != '0':
                return None

            current_price = float(price_data['output'].get('stck_prpr', 0))
            if current_price <= 0:
                return None

            # EPS 정보 조회 시도
            overview_data = await self.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})
                eps = output.get('eps')
                if eps and float(eps) > 0:
                    per = current_price / float(eps)
                    if 0.1 <= per <= 300.0:
                        logger.debug(f"Calculated PER for {stock_code}: {per:.2f}")
                        return per

            return None
            
        except Exception as e:
            logger.error(f"Error calculating PER for {stock_code}: {e}")
            return None
    
    async def calculate_roe(self, stock_code: str) -> Optional[float]:
        """ROE 계산 (자기자본이익률, %) - 새로운 재무지표 API 사용"""
        try:
            # 재무비율 API로 직접 조회
            financial_data = await self.get_financial_ratios(stock_code)
            if financial_data and financial_data.get('rt_cd') == '0':
                output = financial_data.get('output', {})

                # ROE가 직접 제공되는지 확인
                for roe_key in ['roe', 'stck_roe', 'return_on_equity']:
                    roe_value = output.get(roe_key)
                    if roe_value and roe_value != '0' and roe_value != '-':
                        try:
                            roe = float(roe_value)
                            if -100.0 <= roe <= 150.0:  # 합리적 범위
                                logger.debug(f"Direct ROE for {stock_code}: {roe:.2f}%")
                                return roe
                        except (ValueError, TypeError):
                            continue

            # 폴백: 기존 방식으로 계산
            logger.debug(f"Falling back to manual ROE calculation for {stock_code}")
            overview_data = await self.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})

                # PBR과 PER을 이용한 ROE 추정
                pbr = await self.calculate_pbr(stock_code)
                per = await self.calculate_per(stock_code)

                if pbr and per and pbr > 0 and per > 0:
                    estimated_roe = (1 / per) * (1 / pbr) * 100
                    if -50.0 <= estimated_roe <= 100.0:
                        logger.debug(f"Estimated ROE for {stock_code}: {estimated_roe:.2f}%")
                        return estimated_roe

            return None

        except Exception as e:
            logger.error(f"Error calculating ROE for {stock_code}: {e}")
            return None
    
    async def calculate_psr(self, stock_code: str) -> Optional[float]:
        """PSR 계산 (주가매출액비율) - 새로운 재무지표 API 사용"""
        try:
            # 재무비율 API로 직접 조회
            financial_data = await self.get_financial_ratios(stock_code)
            if financial_data and financial_data.get('rt_cd') == '0':
                output = financial_data.get('output', {})

                # PSR이 직접 제공되는지 확인
                for psr_key in ['psr', 'stck_psr', 'price_to_sales']:
                    psr_value = output.get(psr_key)
                    if psr_value and psr_value != '0' and psr_value != '-':
                        try:
                            psr = float(psr_value)
                            if 0.01 <= psr <= 30.0:  # 합리적 범위
                                logger.debug(f"Direct PSR for {stock_code}: {psr:.2f}")
                                return psr
                        except (ValueError, TypeError):
                            continue

            # 폴백: 기존 방식으로 계산
            logger.debug(f"Falling back to manual PSR calculation for {stock_code}")
            price_data = await self.get_current_price(stock_code)
            if not price_data or price_data.get('rt_cd') != '0':
                return None

            current_price = float(price_data['output'].get('stck_prpr', 0))
            if current_price <= 0:
                return None

            # SPS(주당매출액) 정보 조회 시도
            overview_data = await self.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})
                sps = output.get('sps') or output.get('sales_per_share')
                if sps and float(sps) > 0:
                    psr = current_price / float(sps)
                    if 0.01 <= psr <= 30.0:
                        logger.debug(f"Calculated PSR for {stock_code}: {psr:.2f}")
                        return psr

            return None

        except Exception as e:
            logger.error(f"Error calculating PSR for {stock_code}: {e}")
            return None

    # 캐싱 및 폴백 로직이 적용된 메서드들
    async def get_per_cached(self, stock_code: str) -> Optional[float]:
        """PER 조회 (캐싱 + 폴백 로직 적용)"""
        if self.data_manager:
            return await self.data_manager.get_per_with_fallback(stock_code)
        else:
            return await self.calculate_per(stock_code)

    async def get_roe_cached(self, stock_code: str) -> Optional[float]:
        """ROE 조회 (캐싱 + 폴백 로직 적용)"""
        if self.data_manager:
            return await self.data_manager.get_roe_with_fallback(stock_code)
        else:
            return await self.calculate_roe(stock_code)

    async def get_psr_cached(self, stock_code: str) -> Optional[float]:
        """PSR 조회 (캐싱 + 폴백 로직 적용)"""
        if self.data_manager:
            return await self.data_manager.get_psr_with_fallback(stock_code)
        else:
            return await self.calculate_psr(stock_code)

    async def get_pbr_cached(self, stock_code: str) -> Optional[float]:
        """PBR 조회 (캐싱 + 폴백 로직 적용)"""
        if self.data_manager:
            return await self.data_manager.get_pbr_with_fallback(stock_code)
        else:
            return await self.calculate_pbr(stock_code)

    def get_cache_stats(self) -> Dict:
        """캐시 통계 조회"""
        if self.data_manager:
            return self.data_manager.get_cache_stats()
        return {'total': 0, 'valid': 0, 'expired': 0}

    def cleanup_cache(self):
        """만료된 캐시 정리"""
        if self.data_manager:
            self.data_manager.cleanup_cache()

    def get_data_quality_report(self) -> Dict:
        """데이터 품질 보고서 조회"""
        if self.data_manager:
            return self.data_manager.get_quality_report()
        return {}

    def log_data_quality_summary(self):
        """데이터 품질 요약 로그 출력"""
        if self.data_manager:
            self.data_manager.log_quality_summary()

    def get_blacklist_candidates(self, threshold: int = 5) -> List[str]:
        """블랙리스트 후보 종목 조회"""
        if self.data_manager:
            return self.data_manager.get_blacklist_candidates(threshold)
        return []

    def add_stock_to_blacklist(self, stock_code: str, reason: str = "Manual addition"):
        """수동으로 종목을 블랙리스트에 추가"""
        if self.data_manager:
            self.data_manager.add_to_blacklist(stock_code, reason)