"""
매매 모듈
자동매매 로직 및 포지션 관리
"""

# 의존성이 적은 모듈들 먼저 import
from .risk_manager import RiskManager
from .stop_loss_manager import StopLossManager
from .enhanced_trading_system import EnhancedTradingSystem
from .trading_frequency_controller import TradingFrequencyController
from .frequency_dashboard import FrequencyDashboard

# trader는 의존성이 많으므로 조건부 import
try:
    from .trader import AutoTrader, Position
    __all__ = ['AutoTrader', 'Position', 'RiskManager', 'StopLossManager', 'EnhancedTradingSystem', 'TradingFrequencyController', 'FrequencyDashboard']
except ImportError:
    __all__ = ['RiskManager', 'StopLossManager', 'EnhancedTradingSystem', 'TradingFrequencyController', 'FrequencyDashboard']