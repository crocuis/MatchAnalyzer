# 파생시장 표기 정리 설계

## 목표

대시보드 카드에서 `파생 시장` 표기를 제거하고, 모달에서는 현재의 raw 데이터 나열 대신 사람이 읽을 수 있는 간단한 시장 설명으로 바꾼다.

이번 변경의 핵심 목표는 두 가지다.

- 카드에서 정보 위계가 약한 보조 데이터를 제거해 가독성을 높인다.
- 모달에서는 `spreads`, `totals`, `L: -0.5` 같은 내부 표현 대신 해석 가능한 문장으로 보여준다.

## 문제 정의

현재 UI는 `variantMarkets`가 있으면 다음처럼 노출한다.

- 카드: `파생 시장` 라벨 아래 `spreads · totals`
- 모달: `marketFamily`, `lineValue`, `selectionALabel`, `selectionBLabel`, 확률을 그대로 나열

이 구조에는 두 가지 문제가 있다.

- 카드에서는 이 데이터가 핵심 판단 정보처럼 보이지 않는데도 별도 metric 블록을 차지해 시선을 분산시킨다.
- 모달에서는 시장 종류보다 내부 데이터 필드가 먼저 보이기 때문에 사용자가 “그래서 무슨 의미인가”를 다시 해석해야 한다.

즉, 데이터는 있지만 현재 copy와 구조가 사람 중심이 아니다.

## 설계 방향

### 1. 카드에서는 완전히 제거

카드의 목적은 빠른 스캔이다.

따라서 `variantMarkets`가 있더라도 카드 본문에서는 더 이상 별도 행으로 노출하지 않는다.

유지할 정보:

- 예측
- 실제 결과
- 판정
- 가치 베팅 배지
- 리뷰 필요 배지

제거할 정보:

- `파생 시장`
- `spreads · totals` 같은 `marketFamily` 목록

### 2. 모달에서는 “시장 종류 + 해석 문장”으로 변환

모달에서는 `variantMarkets`를 계속 보여주되, raw field 나열이 아니라 아래 구조로 바꾼다.

- 제목: `추가 시장`
- 각 행:
  - 사람이 읽는 시장 종류명
  - 한 줄 설명

예시:

- `핸디캡`
  - `홈 -0.5 우세 · 54% vs 46%`
- `언더/오버`
  - `오버 2.5 우세 · 57% vs 43%`

즉, 사용자는 더 이상 `selectionALabel`, `selectionBLabel`, `marketFamily`를 직접 해석하지 않아도 된다.

## 데이터 변환 규칙

### 1. 시장 종류 라벨

`marketFamily`는 그대로 보여주지 않고 사람이 읽는 라벨로 바꾼다.

기본 매핑:

- `spreads` -> `핸디캡`
- `totals` -> `언더/오버`

매핑이 없는 값은:

- `marketFamily`를 humanize 해서 fallback

예:

- `draw_no_bet` -> `Draw No Bet`

### 2. 설명 문장

한 줄 설명은 아래 규칙으로 만든다.

- `selectionALabel`과 `selectionBLabel` 중 더 높은 확률 쪽을 `우세`로 표현
- `lineValue`가 있으면 라벨 안에 그대로 포함
- 확률은 `%`로 반올림 표시

예:

- `Home -0.5` 0.54 vs `Away +0.5` 0.46
  -> `홈 -0.5 우세 · 54% vs 46%`
- `Over 2.5` 0.57 vs `Under 2.5` 0.43
  -> `오버 2.5 우세 · 57% vs 43%`

둘 다 확률이 없는 경우는 드물지만, 그때는:

- 라벨만 보여주고 확률 부분은 생략

예:

- `오버 2.5 vs 언더 2.5`

## 구현 방향

### 1. 카드

`MatchCard.tsx`에서 `hasVariantMarkets` 계산과 해당 metric 블록을 제거한다.

### 2. 모달

`PredictionCard.tsx` 안에서 `variantMarkets` 렌더링 전에 format helper를 추가한다.

새 helper 책임:

- 시장 종류 라벨 변환
- 우세 selection 판별
- 한 줄 설명 문자열 생성

가능하면 이 helper는 컴포넌트 안의 작은 순수 함수로 두고, 새 추상화는 최소화한다.

### 3. 번역

기존 `variantMarketsTitle`와 `matchCard.variantMarkets`는 의미가 바뀐다.

권장 변경:

- 카드용 `matchCard.variantMarkets`는 더 이상 사용하지 않음
- 모달 제목 `variantMarketsTitle`는 `추가 시장` / `Additional markets` 쪽으로 조정

필요하면 시장 종류 라벨도 locale을 통해 관리할 수 있지만, 이번 단계에서는 helper 내부 매핑으로 충분하다.

## 테스트 전략

### 카드

- `variantMarkets`가 있어도 카드에서 `파생 시장` 또는 `spreads` 텍스트가 더 이상 보이지 않는지 확인

### 모달

- `spreads`가 `핸디캡`으로 보이는지 확인
- `totals`가 `언더/오버`로 보이는지 확인
- 설명 문장이 raw field 나열 대신 해석 문장으로 바뀌는지 확인

## 구현 범위

이번 구현에 포함:

- 카드의 `variantMarkets` 표시 제거
- 모달의 `variantMarkets` copy 재구성
- 관련 테스트 기대값 갱신

이번 구현에서 제외:

- API payload 변경
- 새로운 시장 타입 스키마 확장
- 상세 페이지 별도 시장 분석 섹션 추가

## 리스크

- 시장 종류 매핑이 적으면 일부 fallback 라벨이 영어로 남을 수 있다.
  - 이번 단계에서는 `spreads`, `totals`만 우선 매핑하고 나머지는 humanize fallback으로 둔다.
- selection label 형식이 소스마다 다르면 문장 생성 품질이 들쭉날쭉할 수 있다.
  - 우선 현재 fixture에 있는 label 패턴 기준으로 맞추고, 이후 필요 시 정규화 범위를 넓힌다.

## 권장 후속 작업

- 시장 종류 라벨을 locale 리소스로 이동
- `variantMarkets`가 실제 추천 의미를 가지는지 여부에 따라 별도 confidence/edge 요약으로 재구성
