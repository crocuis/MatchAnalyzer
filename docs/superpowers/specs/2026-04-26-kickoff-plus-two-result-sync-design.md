# kickoff +2H 결과 동기화 분리 설계

## 목적

GitHub Actions 월간 분 사용량을 줄이기 위해 미래 fixture window 동기화와 경기 결과 정산 동기화를 분리한다. 결과 정산은 전체 일자 window를 반복 동기화하지 않고, 경기 시작 후 2시간이 지났지만 아직 `final_result`가 비어 있는 경기만 대상으로 한다.

## 현재 문제

`ingest-fixtures` workflow는 미래 일정 동기화, 스냅샷 생성, 변경 경기 예측 갱신을 함께 수행한다. 이 경로는 평균 실행 시간이 길고 scheduled run 빈도가 높아 월간 분 사용량의 큰 비중을 차지한다. 반면 결과 정산은 이미 DB에 있는 경기 중 일부만 다시 확인하면 된다.

## 설계

새 배치 job `sync_match_results_job`을 추가한다. 이 job은 `matches` 테이블에서 `kickoff_at <= now - 2h`이고 `final_result is null`인 경기만 찾는다. 대상은 오래된 미정산 row가 무한히 재조회되지 않도록 기본 48시간 lookback으로 제한한다.

대상 경기의 kickoff date별로 `fetch_daily_schedule(date)`를 호출하고, 응답 이벤트 중 대상 match id만 골라 `build_match_row_from_event`로 결과 row를 만든다. `final_result`가 채워진 row만 `matches`에 upsert한다. 결과가 새로 관측된 시각은 `result_observed_at`에 현재 시각으로 남긴다.

새 workflow `sync-match-results`는 2시간마다 실행한다. 결과가 새로 들어온 날짜만 대상으로 `run_post_match_review_job`, `run_daily_pick_tracking_job`, `export_daily_pick_artifacts_job`을 실행한다. LLM review는 기본 비활성화하고, 일 1회 `post-match-review`가 보수적 보강 경로로 남는다.

## 데이터 흐름

1. GitHub Actions schedule이 `sync-match-results`를 실행한다.
2. job이 DB에서 미정산 후보를 읽는다.
3. 후보의 날짜별 fixture schedule만 조회한다.
4. 해당 match id의 결과가 닫혀 있으면 `matches`를 upsert한다.
5. 변경된 날짜별로 post-match review와 daily pick settle/export를 수행한다.

## 운영 정책

- 기본 결과 재조회 기준: `kickoff_at + 2h`.
- 기본 lookback: 48시간.
- 결과 provider가 늦게 닫는 경기를 위해 manual dispatch에서 기준 시간을 조정할 수 있다.
- 미래 fixture 동기화 빈도는 낮게 유지하고, 결과 freshness는 새 workflow가 책임진다.

## 검증

- `sync_match_results_job` 단위 테스트로 후보 필터, date별 fetch, 결과 upsert, 빈 결과 skip을 고정한다.
- workflow 테스트로 새 cron과 후속 처리 조건을 고정한다.
- 기존 `npm test`, `python3 -m pytest`, YAML parse 검증을 유지한다.
