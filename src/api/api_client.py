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

        # 캐싱 및 폴백 로직을 위한 데이터 매니저
        self.data_manager = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        await self.get_access_token()

        # 데이터 매니저 초기화
        self.data_manager = FinancialDataManager(self)

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
        """종목 개요 및 재무지표 조회"""
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
    
    async def calculate_pbr(self, stock_code: str) -> Optional[float]:
        """PBR 계산 (주가순자산비율)"""
        try:
            # 현재가 조회
            price_data = await self.get_current_price(stock_code)
            if not price_data or price_data.get('rt_cd') != '0':
                logger.warning(f"Failed to get current price for {stock_code}")
                return None
            
            current_price = float(price_data['output'].get('stck_prpr', 0))
            if current_price <= 0:
                return None
            
            # 재무정보 조회 (대안으로 기본 정보에서 추출)
            overview_data = await self.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})
                
                # PBR이 직접 제공되는지 확인
                if 'pbr' in output or 'per_pbr' in output:
                    pbr_value = output.get('pbr') or output.get('per_pbr')
                    if pbr_value and pbr_value != '0':
                        return float(pbr_value)
                
                # BPS(주당순자산가치)를 이용한 PBR 계산
                bps = output.get('bps')  # Book Value Per Share
                if bps and float(bps) > 0:
                    pbr = current_price / float(bps)
                    logger.debug(f"Calculated PBR for {stock_code}: {pbr:.2f} (Price: {current_price}, BPS: {bps})")
                    return pbr
            
            logger.warning(f"Could not calculate PBR for {stock_code} - insufficient data")
            return None
            
        except Exception as e:
            logger.error(f"Error calculating PBR for {stock_code}: {e}")
            return None
    
    async def calculate_per(self, stock_code: str) -> Optional[float]:
        """PER 계산 (주가수익비율)"""
        try:
            # 현재가 조회
            price_data = await self.get_current_price(stock_code)
            if not price_data or price_data.get('rt_cd') != '0':
                logger.warning(f"Failed to get current price for {stock_code}")
                return None
            
            current_price = float(price_data['output'].get('stck_prpr', 0))
            if current_price <= 0:
                return None
            
            # 종목 개요에서 PER 정보 조회
            overview_data = await self.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})
                
                # PER이 직접 제공되는지 확인
                if 'per' in output:
                    per_value = output.get('per')
                    if per_value and per_value != '0' and per_value != '-':
                        return float(per_value)
                
                # EPS(주당순이익)를 이용한 PER 계산
                eps = output.get('eps')  # Earnings Per Share
                if eps and float(eps) > 0:
                    per = current_price / float(eps)
                    logger.debug(f"Calculated PER for {stock_code}: {per:.2f} (Price: {current_price}, EPS: {eps})")
                    return per
                
                # 연간 순이익과 상장주식수로 PER 계산 (대안)
                net_income = output.get('net_income')  # 순이익
                shares_outstanding = output.get('lstg_st_cnt')  # 상장주식수
                
                if net_income and shares_outstanding:
                    try:
                        net_income_val = float(net_income)
                        shares_val = float(shares_outstanding)
                        if net_income_val > 0 and shares_val > 0:
                            eps_calculated = net_income_val / shares_val
                            per = current_price / eps_calculated
                            logger.debug(f"Calculated PER for {stock_code}: {per:.2f} (from net income)")
                            return per
                    except:
                        pass
            
            logger.warning(f"Could not calculate PER for {stock_code} - insufficient data")
            return None
            
        except Exception as e:
            logger.error(f"Error calculating PER for {stock_code}: {e}")
            return None
    
    async def calculate_roe(self, stock_code: str) -> Optional[float]:
        """ROE 계산 (자기자본이익률, %)"""
        try:
            # 종목 개요에서 ROE 정보 조회
            overview_data = await self.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})
                
                # ROE가 직접 제공되는지 확인
                if 'roe' in output:
                    roe_value = output.get('roe')
                    if roe_value and roe_value != '0' and roe_value != '-':
                        roe_percent = float(roe_value)
                        logger.debug(f"Direct ROE for {stock_code}: {roe_percent:.2f}%")
                        return roe_percent
                
                # 수동 ROE 계산: (순이익 / 자기자본) * 100
                net_income = output.get('net_income')  # 순이익
                equity = output.get('equity') or output.get('stockholders_equity')  # 자기자본
                
                if net_income and equity:
                    try:
                        net_income_val = float(net_income)
                        equity_val = float(equity)
                        if equity_val > 0:
                            roe = (net_income_val / equity_val) * 100
                            logger.debug(f"Calculated ROE for {stock_code}: {roe:.2f}% (NI: {net_income_val}, Equity: {equity_val})")
                            return roe
                    except:
                        pass
                
                # 대안 계산: PBR과 PER을 이용한 ROE 추정
                # ROE ≈ (1/PBR) * (1/PER) * Price * 100 (근사치)
                pbr = await self.calculate_pbr(stock_code)
                per = await self.calculate_per(stock_code)
                
                if pbr and per and pbr > 0 and per > 0:
                    # DuPont 공식 근사: ROE = (Net Income/Sales) * (Sales/Assets) * (Assets/Equity)
                    # 간단한 추정: ROE ≈ (1/PER) * (Price/Book) * 100
                    estimated_roe = (1 / per) * (1 / pbr) * 100
                    if 0 < estimated_roe < 100:  # 합리적 범위 체크
                        logger.debug(f"Estimated ROE for {stock_code}: {estimated_roe:.2f}% (from PBR/PER)")
                        return estimated_roe
            
            logger.warning(f"Could not calculate ROE for {stock_code} - insufficient data")
            return None
            
        except Exception as e:
            logger.error(f"Error calculating ROE for {stock_code}: {e}")
            return None
    
    async def calculate_psr(self, stock_code: str) -> Optional[float]:
        """PSR 계산 (주가매출액비율)"""
        try:
            # 현재가 조회
            price_data = await self.get_current_price(stock_code)
            if not price_data or price_data.get('rt_cd') != '0':
                logger.warning(f"Failed to get current price for {stock_code}")
                return None
            
            current_price = float(price_data['output'].get('stck_prpr', 0))
            if current_price <= 0:
                return None
            
            # 종목 개요에서 PSR 정보 조회
            overview_data = await self.get_stock_overview(stock_code)
            if overview_data and overview_data.get('rt_cd') == '0':
                output = overview_data.get('output', {})
                
                # PSR이 직접 제공되는지 확인
                if 'psr' in output:
                    psr_value = output.get('psr')
                    if psr_value and psr_value != '0' and psr_value != '-':
                        psr = float(psr_value)
                        logger.debug(f"Direct PSR for {stock_code}: {psr:.2f}")
                        return psr
                
                # 수동 PSR 계산: 시가총액 / 매출액
                shares_outstanding = output.get('lstg_st_cnt')  # 상장주식수
                revenue = output.get('revenue') or output.get('sales') or output.get('total_revenue')  # 매출액
                
                if shares_outstanding and revenue:
                    try:
                        shares_val = float(shares_outstanding)
                        revenue_val = float(revenue)
                        if revenue_val > 0 and shares_val > 0:
                            market_cap = current_price * shares_val
                            psr = market_cap / revenue_val
                            logger.debug(f"Calculated PSR for {stock_code}: {psr:.2f} (MarketCap: {market_cap}, Revenue: {revenue_val})")
                            return psr
                    except:
                        pass
                
                # 대안 계산: 주당매출액(SPS)을 이용한 PSR 계산
                sps = output.get('sps') or output.get('sales_per_share')  # Sales Per Share
                if sps and float(sps) > 0:
                    psr = current_price / float(sps)
                    logger.debug(f"Calculated PSR for {stock_code}: {psr:.2f} (Price: {current_price}, SPS: {sps})")
                    return psr
                
                # 최후 추정: PER과 순이익률을 이용한 PSR 추정
                # PSR ≈ PER × Net Margin (순이익률)
                per = await self.calculate_per(stock_code)
                if per and revenue:
                    try:
                        net_income = output.get('net_income')
                        if net_income and float(revenue) > 0:
                            net_margin = float(net_income) / float(revenue)
                            if 0 < net_margin < 1:  # 합리적 순이익률 범위
                                estimated_psr = per * net_margin
                                if 0 < estimated_psr < 20:  # 합리적 PSR 범위
                                    logger.debug(f"Estimated PSR for {stock_code}: {estimated_psr:.2f} (from PER × Net Margin)")
                                    return estimated_psr
                    except:
                        pass
            
            logger.warning(f"Could not calculate PSR for {stock_code} - insufficient data")
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