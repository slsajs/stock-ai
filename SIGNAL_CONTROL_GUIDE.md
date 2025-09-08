# 🎛️ 신호 분석기 점수 제어 가이드

## 📋 개요
이제 강화된 신호 분석기의 점수 기준과 가중치를 **실시간으로 조정**할 수 있습니다!

## 🚀 사용법

### 1️⃣ 명령줄 인자로 조정

```bash
# 점수 기준을 47점으로 낮춰서 실행
python main.py --score 47

# 여러 설정을 동시에 조정
python main.py --score 50 --rsi-weight 40 --volume-weight 20

# 최소 수익률을 0.5%로 낮춰서 실행
python main.py --score 45 --min-profit 0.005

# 테스트 모드로 새 설정 확인
python main.py test --score 47
```

### 2️⃣ 환경변수로 조정

`.env` 파일에 추가:
```bash
# 강화된 신호 분석기 설정
SIGNAL_SCORE_THRESHOLD=50          # 매수 신호 최소 점수
RSI_WEIGHT=35                      # RSI 가중치 증가
MACD_WEIGHT=30                     # MACD 가중치 증가  
BOLLINGER_WEIGHT=15                # 볼린저밴드 가중치 감소
VOLUME_WEIGHT=15                   # 거래량 가중치 유지
TREND_WEIGHT=5                     # 추세 가중치 감소
MIN_TARGET_PROFIT_RATE=0.005       # 최소 목표 수익률 0.5%
```

## 🎯 추천 설정

### 🔥 적극적 매매 (더 많은 신호)
```bash
python main.py --score 40 --min-profit 0.004
```

### ⚖️ 균형잡힌 매매 (현재 최고점 기준)
```bash
python main.py --score 47 --min-profit 0.006
```

### 🛡️ 보수적 매매 (엄선된 신호)
```bash
python main.py --score 60 --min-profit 0.01
```

### 📊 거래량 중심 전략
```bash
python main.py --score 45 --volume-weight 25 --rsi-weight 25
```

### 📈 기술적 분석 중심 전략
```bash
python main.py --score 50 --rsi-weight 35 --macd-weight 30 --volume-weight 10
```

## 📊 현재 상황 분석

**최근 로그 분석 결과:**
- **최고 점수**: 47점 (종목: 100090)
- **평균 점수**: 약 30점
- **기존 기준**: 80점 (매매 불가능)

**추천 조치:**
1. **임시적으로 47점**으로 설정하여 매매 재개
2. 며칠간 실제 성과 모니터링
3. 점진적으로 기준 상향 조정

## 🔄 실시간 조정 방법

### 현재 실행 중인 시스템 재시작
```bash
# Ctrl+C로 중지 후
python main.py --score 47  # 새 점수로 재시작
```

### 빠른 테스트
```bash
python main.py test --score 30  # 30점 기준으로 테스트
python main.py test --score 40  # 40점 기준으로 테스트  
python main.py test --score 50  # 50점 기준으로 테스트
```

## 💡 사용 팁

1. **단계적 조정**: 80 → 60 → 50 → 47 순으로 점진적 하향
2. **실시간 모니터링**: 로그에서 "종합점수" 확인
3. **백테스팅**: test 모드로 먼저 확인
4. **성과 추적**: 며칠간 결과 모니터링 후 조정

## ⚠️ 주의사항

- **너무 낮은 점수**: 품질 낮은 신호 증가
- **가중치 합계**: 100%가 되도록 조정
- **최소 수익률**: 수수료(~0.3%) 고려하여 설정
- **시장 상황**: 변동성 높은 날에는 점수 기준 상향

## 📈 모니터링 방법

실행 중 로그에서 다음 확인:
```
🔍 Enhanced Analysis 100090:
  • 종합점수: 47.0/100 (최소:47)  ← 새 기준으로 통과!
  • 결과: 매수 조건 만족
```

성공적으로 점수 기준이 조정되어 매매가 재개됩니다! 🎉