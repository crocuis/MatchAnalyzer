# 대시보드 아이콘 중심 카드 재설계

## 목표

대시보드 카드에서 텍스트 의존도를 줄이고, 사용자가 한눈에 `예측 결과`, `실제 결과`, `판정 상태`, `운영 이슈`를 읽을 수 있도록 정보 위계를 재구성한다.

이번 설계는 카드, 모달, 상세 페이지가 같은 정보를 반복하는 구조를 줄이고 다음 원칙을 따른다.

- 카드: 무슨 일이 일어났는지 즉시 읽힘
- 모달: 핵심 요약과 상태 해석
- 상세: 근거 수치와 누락 원인, 동기화 보강 정보

## 문제 정의

현재 카드와 모달은 다음 문제가 있다.

- 카드가 팀명, 신뢰도, 상태, 이유 태그를 모두 텍스트로 보여줘서 시선 우선순위가 약하다.
- 예측과 실제 결과의 비교가 카드 첫 화면에서 즉시 읽히지 않는다.
- 리뷰 필요, 시장 없음, 라인업 미확정, 동기화 누락 같은 운영 상태가 텍스트 뭉치에 묻힌다.
- 누락 신호는 단순 `missing_fields` 목록이라 운영자가 “왜 빠졌는지”와 “동기화 때 뭘 더 넣어야 하는지”를 바로 판단하기 어렵다.

## 설계 방향

### 1. 카드 구조를 `결과 보드 + 운영 상태`로 분리

카드 상단은 3칸 결과 보드로 재구성한다.

- `Predicted`: 예측 아이콘 + `H/D/A`
- `Actual`: 실제 결과 아이콘 + `H/D/A` 또는 `-`
- `Verdict`: 적중 / 미스 / no-bet / 예정 상태 아이콘

카드 하단은 운영 상태 아이콘만 배치한다.

- 리뷰 필요
- 시장 데이터 없음
- 라인업 미확정
- 동기화 누락

팀명과 날짜는 유지하되, 결과 보드보다 한 단계 낮은 시각적 위계로 내린다.

### 2. 아이콘은 문자 보조를 포함한다

아이콘만으로 해석 비용이 커지지 않도록 문자와 함께 표시한다.

- 홈: 집 아이콘 + `H`
- 무: 중립/원형 아이콘 + `D`
- 원정: 방향/원정 아이콘 + `A`

상태 아이콘은 아래와 같이 통일한다.

- 적중: 체크
- 미스: 경고 또는 `X`
- no-bet: 일시정지
- 예정: 시계
- 리뷰 필요: 배지/노트
- 시장 없음: 끊긴 차트
- 라인업 미확정: 사용자 실루엣
- 동기화 누락: DB 경고 또는 broken link

### 3. 카드 텍스트는 “짧은 문장 + 아이콘 배지”로 축약

카드에서는 긴 reason tag 나열을 제거하고, 아래 두 축만 남긴다.

- 짧은 핵심 문장 1개
  - 예: `추천 보류, 시장과 라인업 근거가 약함`
  - 예: `홈 우세 예측, 핵심 신호는 정렬됨`
- 운영 배지 2~4개
  - `시장 없음`
  - `라인업 미확정`
  - `신호 충돌`
  - `동기화 누락`

카드에서는 raw signal key와 calibration 수치를 직접 노출하지 않는다.

### 4. 모달은 “요약 우선, 근거 보조”로 유지

모달 상단에도 카드와 같은 결과 보드를 넣어 정보 연속성을 만든다.

그 아래에는 다음 순서로 요약한다.

- 핵심 문장
- 상태 배지
- 누락 신호 요약
  - `누락 신호 4개`
  - `폼 동기화 누락`
  - `라인업 동기화 누락`

모달에서는 수치 전체를 다 보여주지 않고, 대표 원인과 동기화 보강 방향만 보여준다.

### 5. 상세 페이지는 현재 수준의 분석 화면을 유지하되 해석 층을 추가

상세 페이지에서는 기존 수치와 breakdown을 유지한다.

다만 `missing_fields`를 그대로 노출하는 대신 아래 정보를 우선 배치한다.

- 누락 신호 개수
- 누락 원인 분류
- sync 보강 권장 액션

예:

- `폼 동기화 누락`
- `일정 컨텍스트 누락`
- `PL 외 결장 피드 확장 필요`

## 데이터 설계

### 1. 카드용 파생 상태

카드에서 직접 계산하거나 직렬화해도 되는 파생값은 아래와 같다.

- `predictedOutcomeDisplay`
- `actualOutcomeDisplay`
- `verdictState`
- `statusIconKeys[]`

이 값은 프론트에서 계산해도 되지만, 기존 payload를 유지하는 방향을 우선한다.

### 2. 누락 신호 메타데이터 확장

`feature_metadata`는 기존 `missing_fields`만으로는 운영 판단이 어렵다.

따라서 아래 구조를 추가한다.

```json
{
  "missing_signal_reasons": [
    {
      "reason_key": "form_context_missing",
      "fields": ["home_points_last_5", "away_points_last_5"],
      "explanation": "Recent form points were not synced into the snapshot.",
      "sync_action": "Persist recent five-match points during fixture snapshot sync."
    }
  ]
}
```

표준 reason taxonomy:

- `form_context_missing`
- `schedule_context_missing`
- `rating_context_missing`
- `xg_context_missing`
- `lineup_context_missing`
- `absence_feed_missing`

## 데이터 동기화 분석

현재 누락 신호의 핵심 원인은 세 가지다.

### 1. snapshot에 아예 저장되지 않는 raw signal

기존에는 `form_delta`, `rest_delta`가 일부 케이스에서 snapshot 정본이 아니라 런타임 fallback으로 채워졌다.

이건 카드/모달/UI 문제가 아니라 snapshot 생성 단계의 정본성 문제다.

우선 조치:

- `home_points_last_5`
- `away_points_last_5`
- `home_rest_days`
- `away_rest_days`

를 snapshot 생성 시 함께 저장한다.

### 2. lineup / absence source coverage 부족

현재 lineup과 absence는 source coverage가 제한적이다.

- lineup: 이벤트 시점 lineups 의존
- absence: 사실상 특정 리그 중심 coverage

따라서 다음 reason이 자주 생긴다.

- `lineup_context_missing`
- `absence_feed_missing`

우선 조치:

- lineup source summary를 match별로 항상 저장
- non-PL competition에도 absence source 확장 검토

### 3. historical window 부족

다음 신호는 historical data window가 얕으면 빠진다.

- Elo
- rolling xG proxy
- 최근 7일 경기 수

따라서:

- `rating_context_missing`
- `xg_context_missing`
- `schedule_context_missing`

이 발생한다.

우선 조치:

- 예측 실행 전 historical snapshot/backfill 범위 보강
- 시즌 초반 및 신규 competition에 대한 최소 history 확보

## 구현 범위

이번 구현 범위:

- 카드 결과 보드 도입
- 아이콘 중심 상태 표현
- 카드 핵심 문장 + 상태 배지 구조 도입
- 모달의 누락 신호 요약 섹션 도입
- 상세의 missing signal reason / sync action 노출
- snapshot에 `form/rest` raw signal 저장
- `feature_metadata.missing_signal_reasons` 생성

이번 범위에서 제외:

- 새로운 외부 데이터 공급자 추가
- non-PL absence source 실제 확장
- 아이콘 자산을 별도 디자인 시스템으로 분리
- 결과 보드 전용 서버 직렬화 필드 추가

## 테스트 전략

### 프론트

- 카드에서 예측 / 실제 / 판정 상태가 동시에 읽히는지 테스트
- no-bet / scheduled / review required 상태가 아이콘/텍스트로 일관되게 보이는지 테스트
- 모달에서 누락 신호 개수와 대표 원인이 노출되는지 테스트

### 배치

- snapshot 생성 시 `home_points_last_5`, `away_points_last_5`, `home_rest_days`, `away_rest_days`가 저장되는지 테스트
- `missing_signal_reasons`가 올바른 taxonomy로 분류되는지 테스트
- 기존 feature metadata와 explanation payload가 깨지지 않는지 회귀 테스트

## 리스크

- 카드의 텍스트를 너무 줄이면 신규 사용자가 해석하기 어려울 수 있다.
  - 완전 아이콘-only 대신 문자 보조를 유지한다.
- 무시장/no-bet 경기와 리뷰 상태가 동시에 있는 카드에서 정보 과밀이 다시 생길 수 있다.
  - 결과 보드와 운영 상태를 시각적으로 분리한다.
- 기존 테스트 fixture가 예전 카드 문구를 전제하고 있어 회귀 테스트 수정이 필요하다.

## 권장 후속 작업

- 카드용 아이콘 매핑을 공용 유틸로 정리
- 상세 페이지에서 reason taxonomy별 집계 필터 추가
- absence feed 확장 전까지 competition별 누락률 리포트 추가
