import os
import logging
from dataclasses import dataclass
from typing import List, Optional
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
try:
    from telegram import Bot
    from telegram.error import TelegramError
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

@dataclass
class MarketAnalysisConfig:
    use_etf_for_index: bool = True
    kospi_etf_code: str = "122630"  # KODEX 코스피
    kosdaq_etf_code: str = "233740"  # KODEX 코스닥150
    crash_threshold: float = -2.0
    strong_bullish_threshold: float = 1.5
    weak_bearish_threshold: float = -1.0
    high_volatility_threshold: float = 35.0
    cache_duration_minutes: int = 10
    api_timeout_seconds: int = 30
    fallback_cache_hours: int = 2

@dataclass
class TradingConfig:
    max_positions: int = 3
    stop_loss_pct: float = -3.0
    take_profit_pct: float = 5.0
    rsi_oversold: int = 30
    rsi_overbought: int = 70
    volume_multiplier: float = 3.0
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    market_analysis: MarketAnalysisConfig = None
    
    def __post_init__(self):
        if self.market_analysis is None:
            self.market_analysis = MarketAnalysisConfig()
    
    @classmethod
    def from_env(cls):
        return cls(
            max_positions=int(os.getenv('MAX_POSITIONS', 3)),
            stop_loss_pct=float(os.getenv('STOP_LOSS_PCT', -3.0)),
            take_profit_pct=float(os.getenv('TAKE_PROFIT_PCT', 5.0)),
            rsi_oversold=int(os.getenv('RSI_OVERSOLD', 30)),
            rsi_overbought=int(os.getenv('RSI_OVERBOUGHT', 70)),
            volume_multiplier=float(os.getenv('VOLUME_MULTIPLIER', 3.0)),
            telegram_bot_token=os.getenv('TELEGRAM_BOT_TOKEN', ''),
            telegram_chat_id=os.getenv('TELEGRAM_CHAT_ID', ''),
            market_analysis=MarketAnalysisConfig(
                use_etf_for_index=os.getenv('USE_ETF_FOR_INDEX', 'true').lower() == 'true',
                kospi_etf_code=os.getenv('KOSPI_ETF_CODE', '122630'),
                kosdaq_etf_code=os.getenv('KOSDAQ_ETF_CODE', '233740'),
                crash_threshold=float(os.getenv('CRASH_THRESHOLD', -2.0)),
                strong_bullish_threshold=float(os.getenv('STRONG_BULLISH_THRESHOLD', 1.5)),
                weak_bearish_threshold=float(os.getenv('WEAK_BEARISH_THRESHOLD', -1.0)),
                high_volatility_threshold=float(os.getenv('HIGH_VOLATILITY_THRESHOLD', 35.0)),
                cache_duration_minutes=int(os.getenv('CACHE_DURATION_MINUTES', 10)),
                api_timeout_seconds=int(os.getenv('API_TIMEOUT_SECONDS', 30)),
                fallback_cache_hours=int(os.getenv('FALLBACK_CACHE_HOURS', 2))
            )
        )
    
    @classmethod
    def from_config_file(cls, config_file: str = "config.json"):
        """설정 파일에서 구성 로드"""
        config_data = load_config_from_file(config_file)
        if not config_data:
            return cls.from_env()
        
        trading_config = config_data.get('trading', {})
        market_config = config_data.get('market_analysis', {})
        
        return cls(
            max_positions=trading_config.get('max_positions', 3),
            stop_loss_pct=trading_config.get('stop_loss_pct', -3.0),
            take_profit_pct=trading_config.get('take_profit_pct', 5.0),
            rsi_oversold=trading_config.get('rsi_oversold', 30),
            rsi_overbought=trading_config.get('rsi_overbought', 70),
            volume_multiplier=trading_config.get('volume_multiplier', 3.0),
            telegram_bot_token=os.getenv('TELEGRAM_BOT_TOKEN', ''),
            telegram_chat_id=os.getenv('TELEGRAM_CHAT_ID', ''),
            market_analysis=MarketAnalysisConfig(
                use_etf_for_index=market_config.get('use_etf_for_index', True),
                kospi_etf_code=market_config.get('kospi_etf_code', '122630'),
                kosdaq_etf_code=market_config.get('kosdaq_etf_code', '233740'),
                crash_threshold=market_config.get('crash_threshold', -2.0),
                strong_bullish_threshold=market_config.get('strong_bullish_threshold', 1.5),
                weak_bearish_threshold=market_config.get('weak_bearish_threshold', -1.0),
                high_volatility_threshold=market_config.get('high_volatility_threshold', 35.0),
                cache_duration_minutes=market_config.get('cache_duration_minutes', 10),
                api_timeout_seconds=market_config.get('api_timeout_seconds', 30),
                fallback_cache_hours=market_config.get('fallback_cache_hours', 2)
            )
        )

def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """RSI 계산"""
    if len(prices) < period + 1:
        return None
    
    # 가격 변화량 계산
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    
    # 상승과 하락 분리
    gains = [delta if delta > 0 else 0 for delta in deltas]
    losses = [-delta if delta < 0 else 0 for delta in deltas]
    
    if len(gains) < period or len(losses) < period:
        return None
    
    # 초기 평균 계산
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_sma(prices: List[float], period: int) -> Optional[float]:
    """단순 이동평균 계산"""
    if len(prices) < period:
        return None
    
    return sum(prices[-period:]) / period

def calculate_ema(prices: List[float], period: int) -> Optional[float]:
    """지수 이동평균 계산"""
    if len(prices) < period:
        return None
    
    multiplier = 2 / (period + 1)
    ema = prices[0]
    
    for price in prices[1:]:
        ema = (price * multiplier) + (ema * (1 - multiplier))
    
    return ema

def calculate_bollinger_bands(prices: List[float], period: int = 20, std_dev: int = 2):
    """볼린저 밴드 계산"""
    if len(prices) < period:
        return None, None, None
    
    recent_prices = prices[-period:]
    sma = sum(recent_prices) / period
    
    # 표준편차 계산
    variance = sum((price - sma) ** 2 for price in recent_prices) / period
    std = variance ** 0.5
    
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    
    return upper_band, sma, lower_band

async def send_telegram_message(message: str, config: TradingConfig):
    """텔레그램 메시지 전송"""
    if not TELEGRAM_AVAILABLE:
        logging.info(f"Telegram not available. Message: {message}")
        return
        
    if not config.telegram_bot_token or not config.telegram_chat_id:
        logging.warning(f"Telegram credentials not configured. Message: {message}")
        return
    
    try:
        bot = Bot(token=config.telegram_bot_token)
        await bot.send_message(chat_id=config.telegram_chat_id, text=message)
        logging.info("Telegram message sent successfully")
    except Exception as e:
        logging.error(f"Failed to send telegram message: {e}")
        logging.info(f"Message was: {message}")

def setup_logging(level: str = "INFO"):
    """로깅 설정"""
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # 로그 포맷 설정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    
    # 파일 핸들러
    file_handler = logging.FileHandler('trading.log', encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # 외부 라이브러리 로그 레벨 조정
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.WARNING)

def validate_stock_code(stock_code: str) -> bool:
    """종목코드 유효성 검사"""
    if not stock_code or len(stock_code) != 6:
        return False
    
    return stock_code.isdigit()

def format_price(price: float) -> str:
    """가격 포맷팅"""
    return f"{price:,.0f}원"

def format_percentage(value: float) -> str:
    """퍼센테이지 포맷팅"""
    return f"{value:+.2f}%"

def calculate_position_size(available_cash: float, price: float, max_positions: int) -> int:
    """포지션 크기 계산"""
    position_amount = available_cash / max_positions
    quantity = int(position_amount / price)
    return max(1, quantity)

def is_trading_day() -> bool:
    """거래일인지 확인 (간단한 주말 체크)"""
    from datetime import datetime
    today = datetime.now().weekday()
    return today < 5  # 월-금 (0-4)

def save_daily_summary(trades: List[dict], filename: str = None):
    """일일 거래 요약 저장"""
    if not trades:
        return
    
    if filename is None:
        from datetime import datetime
        filename = f"daily_summary_{datetime.now().strftime('%Y%m%d')}.csv"
    
    try:
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(trades)
            
            summary = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'total_trades': len(trades),
                'winning_trades': len([t for t in trades if t.get('profit_loss', 0) > 0]),
                'losing_trades': len([t for t in trades if t.get('profit_loss', 0) < 0]),
                'total_pnl': sum(t.get('profit_loss', 0) for t in trades),
                'win_rate': len([t for t in trades if t.get('profit_loss', 0) > 0]) / len(trades) * 100 if trades else 0
            }
            
            summary_df = pd.DataFrame([summary])
            summary_df.to_csv(filename, index=False)
        else:
            # pandas 없이 CSV 저장
            import csv
            from datetime import datetime
            
            summary = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'total_trades': len(trades),
                'winning_trades': len([t for t in trades if t.get('profit_loss', 0) > 0]),
                'losing_trades': len([t for t in trades if t.get('profit_loss', 0) < 0]),
                'total_pnl': sum(t.get('profit_loss', 0) for t in trades),
                'win_rate': len([t for t in trades if t.get('profit_loss', 0) > 0]) / len(trades) * 100 if trades else 0
            }
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=summary.keys())
                writer.writeheader()
                writer.writerow(summary)
        
        logging.info(f"Daily summary saved to {filename}")
        
    except Exception as e:
        logging.error(f"Failed to save daily summary: {e}")

class PriceBuffer:
    """가격 데이터 버퍼"""
    
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.data = []
    
    def add(self, price: float, timestamp=None):
        """가격 데이터 추가"""
        from datetime import datetime
        
        if timestamp is None:
            timestamp = datetime.now()
        
        self.data.append({
            'price': price,
            'timestamp': timestamp
        })
        
        if len(self.data) > self.max_size:
            self.data = self.data[-self.max_size:]
    
    def get_prices(self) -> List[float]:
        """가격 리스트 반환"""
        return [item['price'] for item in self.data]
    
    def get_latest_price(self) -> Optional[float]:
        """최신 가격 반환"""
        return self.data[-1]['price'] if self.data else None
    
    def clear(self):
        """버퍼 초기화"""
        self.data.clear()

def create_trades_csv_if_not_exists():
    """거래 기록 CSV 파일이 없으면 생성"""
    filename = 'trades.csv'
    if not os.path.exists(filename):
        headers = [
            'timestamp', 'stock_code', 'action', 'quantity', 
            'buy_price', 'sell_price', 'profit_loss', 'profit_loss_pct'
        ]
        
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(columns=headers)
            df.to_csv(filename, index=False)
        else:
            import csv
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
        
        logging.info(f"Created {filename}")

def load_config_from_file(config_file: str = "config.json") -> Optional[dict]:
    """설정 파일에서 구성 로드"""
    import json
    
    if not os.path.exists(config_file):
        return None
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load config from {config_file}: {e}")
        return None