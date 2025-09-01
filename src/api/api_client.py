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
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        await self.get_access_token()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.websocket:
            await self.websocket.close()
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
        """현재가 조회"""
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
        """주문 실행"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash"
        
        tr_id = "VTTC0802U" if order_type == "buy" else "VTTC0801U"  # 모의투자
        if not self.is_demo:
            tr_id = "TTTC0802U" if order_type == "buy" else "TTTC0801U"  # 실투자
            
        headers = self._get_headers(tr_id)
        data = {
            "CANO": self.account_no.split("-")[0],
            "ACNT_PRDT_CD": self.account_no.split("-")[1],
            "PDNO": stock_code,
            "ORD_DVSN": "01" if price > 0 else "01",  # 01: 지정가, 01: 시장가
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price) if price > 0 else "0"
        }
        
        return await self._request("POST", url, headers, data)
    
    async def get_balance(self) -> Dict:
        """잔고 조회"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
        headers = self._get_headers("VTTC8434R" if self.is_demo else "TTTC8434R")
        params = {
            "CANO": self.account_no.split("-")[0],
            "ACNT_PRDT_CD": self.account_no.split("-")[1],
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
        """거래량 순위 조회"""
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
        """WebSocket 연결"""
        try:
            if not approval_key:
                approval_key = await self.get_websocket_approval_key()
                if not approval_key:
                    raise Exception("Failed to get WebSocket approval key")
            
            self.approval_key = approval_key
            logger.debug(f"Using approval key: {approval_key}")
            logger.debug(f"Attempting WebSocket connection to: {self.ws_url}")
            self.websocket = await websockets.connect(self.ws_url)
            logger.info("WebSocket connected successfully")
            logger.debug(f"WebSocket state: {self.websocket.state}")
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            logger.debug(f"WebSocket URL: {self.ws_url}")
            raise
    
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
        """실시간 현재가 구독"""
        if not self.websocket:
            raise Exception("WebSocket not connected")
        
        logger.debug(f"Subscribing to {len(stock_codes)} stock codes: {stock_codes}")
        
        for stock_code in stock_codes:
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
        """WebSocket 메시지 수신"""
        if not self.websocket:
            raise Exception("WebSocket not connected")
        
        logger.info("Starting WebSocket message listener")
        message_count = 0
        
        try:
            async for message in self.websocket:
                message_count += 1
                logger.debug(f"Received WebSocket message #{message_count}")
                logger.debug(f"Message type: {type(message)}, length: {len(message) if message else 0}")
                
                if message:
                    logger.debug(f"Raw message (first 200 chars): {str(message)}...")
                
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
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
        except Exception as e:
            logger.error(f"WebSocket listener error: {e}")
            logger.debug(f"Total messages processed: {message_count}")