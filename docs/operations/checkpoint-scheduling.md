# 목표 체크포인트 정책

체크포인트 기준 운영 원칙은 아래와 같다.

- `T-24H`: 시작까지 24~30시간 남은 경기를 대상으로 하루 1회 실행한다.
- `T-6H`: 시작까지 6~8시간 남은 경기를 대상으로 매시간 실행한다.
- `T-1H`: 시작까지 70분 이내인 경기를 대상으로 15분마다 실행한다.
- `LINEUP_CONFIRMED`: 라인업 상태가 `unknown`에서 `confirmed`로 바뀐 경기를 10분마다 감지해 큐에 적재한다.

# 현재 구현된 자동화 매핑

- fixtures ingestion: 매시 `15분`에 실행한다.
- market ingestion: `30분`마다 실행한다.
- prediction batch: 매시 `05분`에 실행한다.
- post-match review: 매시 `45분`에 실행한다.

세부 checkpoint 분기와 `LINEUP_CONFIRMED` 전용 감지 로직은 현재 별도 워크플로로 분리하지 않았고, prediction batch 내부 분기 또는 후속 전용 작업으로 확장할 계획이다.
