# 성능 예산

운영 환경에서는 아래 기준을 기본 성능 예산으로 사용한다.

- `/health`: p95 `100 ms` 이하
- `/matches`: 캐시 히트 시 p95 `150 ms` 이하, 캐시 미스 시 p95 `600 ms` 이하
- `/predictions/:matchId`: 캐시 히트 시 p95 `250 ms` 이하, 캐시 미스 시 p95 `800 ms` 이하
- prediction batch: `5분` 이내
- market ingestion batch: `10분` 이내
- post-match review batch: `10분` 이내

성능 이슈가 발생하면 아래 순서로 대응한다.

1. 응답 페이로드 크기와 조회 컬럼 수를 먼저 줄인다.
2. Worker 캐시 적중률과 materialized read model을 개선한다.
3. 그래도 해결되지 않으면 필요한 특정 병목 경로만 Go 또는 Rust로 재작성한다.
