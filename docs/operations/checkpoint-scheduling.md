# 체크포인트 스케줄링

현재 자동화는 배치 워크플로 기준으로 아래처럼 동작한다. 더 세밀한 체크포인트 분리는 후속 작업에서 강화한다.

- fixtures ingestion: 매시 `15분`에 실행한다.
- market ingestion: `30분`마다 실행한다.
- prediction batch: 매시 `05분`에 실행한다.
- post-match review: 매시 `45분`에 실행한다.

운영 해석 기준은 다음과 같다.

- `T-24H`, `T-6H`, `T-1H`는 현재 하나의 prediction batch 안에서 분기될 예정이며, 세부 대상 추출 로직은 후속 구현 작업에서 추가한다.
- `LINEUP_CONFIRMED` 전용 감지 워크플로는 아직 별도로 두지 않았고, 현재는 후속 구현 범위로 남겨둔다.
