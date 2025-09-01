# 프로젝트 구조

```
stock-ai/
├── main.py                 # 메인 실행 파일
├── setup.py               # 패키지 설정
├── requirements.txt       # 의존성 목록
├── .env                  # 환경 변수 (API 키 등)
├── config.json           # 매매 설정
├── trades.csv           # 거래 기록
├── trading.log          # 시스템 로그
└── src/                 # 소스 패키지
    ├── __init__.py
    ├── api/             # API 모듈
    │   ├── __init__.py
    │   └── api_client.py    # KIS API 클라이언트
    ├── analysis/        # 분석 모듈  
    │   ├── __init__.py
    │   ├── technical_analyzer.py  # 기술적 분석
    │   └── data_manager.py        # 데이터 관리
    ├── trading/         # 매매 모듈
    │   ├── __init__.py
    │   └── trader.py           # 자동매매 로직
    └── utils/           # 유틸리티
        ├── __init__.py
        └── utils.py            # 공통 기능
```

## 모듈별 역할

### 📡 API 모듈 (`src/api/`)
- `KISAPIClient`: 한국투자증권 REST API 및 WebSocket 연동
- 실시간 시세 데이터 수신, 주문 실행

### 📊 분석 모듈 (`src/analysis/`)
- `TechnicalAnalyzer`: RSI, 이동평균, 거래량 분석
- `DataManager`: 실시간 데이터 저장 및 거래 기록 관리

### 💰 매매 모듈 (`src/trading/`)
- `AutoTrader`: 자동매매 엔진
- `Position`: 포지션 관리 데이터 클래스

### 🔧 유틸리티 (`src/utils/`)
- `TradingConfig`: 설정 관리
- 로깅, 텔레그램 알림 등 공통 기능

## 실행 방법

```bash
# 패키지 모드로 설치
pip install -e .

# 메인 시스템 실행
python main.py

# API 연결 테스트
python main.py test
```