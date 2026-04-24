# 목표 체크포인트 정책

체크포인트 기준 운영 원칙은 아래와 같다.

- `T-24H`: 시작까지 24~30시간 남은 경기를 대상으로 하루 1회 실행한다.
- `T-6H`: 시작까지 6~8시간 남은 경기를 대상으로 매시간 실행한다.
- `T-1H`: 시작까지 70분 이내인 경기를 대상으로 15분마다 실행한다.
- `LINEUP_CONFIRMED`: 라인업 상태가 `unknown`에서 `confirmed`로 바뀐 경기를 10분마다 감지해 큐에 적재한다.

# 현재 구현된 자동화 매핑 (샘플/스모크 전용)

- fixtures ingestion: 매시 `00분`에 실행하며, 기준 UTC 날짜 결과 갱신과 `+7일`부터 `+14일`까지의 예정 경기 선행 동기화를 함께 수행한다.
- market ingestion: 매시 `15분`에 실행한다.
- prediction refresh: fixtures/markets ingest 결과에서 실제로 변경된 `match_id` 가 있을 때만 즉시 후속 실행한다.
- 수동 prediction workflow: 필요할 때 `target_date` 또는 `target_match_ids` 로 직접 실행한다.
- post-match review: 매시 `45분`에 실행한다.

세부 checkpoint 분기와 `LINEUP_CONFIRMED` 전용 감지 로직은 현재 별도 워크플로로 분리하지 않았고, prediction batch 내부 분기 또는 후속 전용 작업으로 확장할 계획이다.

현재 GitHub Actions 자동화는 샘플/스모크 파이프라인 기준이다. 실제 운영 데이터 소스나 운영용 Supabase 프로젝트에 그대로 연결하는 것을 전제로 하지 않는다.

# Polymarket 운영 메모

- 현재 `prediction_market` 적재는 `moneyline` 3-way sibling market을 하나의 정규화 row로 접는 방식만 다룬다.
- 지원 범위는 보수적으로 `epl`, `ucl`, `uel`, `kor` 계열에 한정한다.
- `prediction_market`가 없어도 bookmaker fallback으로 prediction/review는 계속 진행된다.
- review 경로는 `prediction_market -> bookmaker -> no-market` 순서로 비교 대상을 고른다.
- `ucl`은 broad search 노이즈가 있을 수 있으므로 팀명 query나 slug 확인이 필요하다.
