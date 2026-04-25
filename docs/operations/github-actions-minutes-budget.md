# GitHub Actions 분 예산 운영 계획

GitHub Actions 월 2000분 제한을 기준으로 CI와 운영 배치를 운영한다. 기본 목표는 월 사용량을 1500분 이하로 유지하고, 500분은 수동 재실행, 장애 대응, 배포 재시도 여유분으로 남기는 것이다.

## 현재 정책

- 정기 배치 실행 횟수를 월 약 510회 수준으로 제한한다.
- `report-missing-signal-coverage`는 각 ingest/prediction 완료마다 실행하지 않고 하루 1회 실행한다.
- PR/push 검증은 단위 테스트와 pytest 중심으로 유지하고, 실제 배치 job 실행은 정기 운영 워크플로가 담당한다.
- 오래된 정기 실행은 취소하고 최신 실행을 우선한다.
- Python 의존성은 `actions/setup-python` pip 캐시를 사용한다.
- 문서만 변경한 `main` push는 테스트 워크플로를 실행하지 않는다.

## 월간 실행 예산

| 워크플로 | 빈도 | 월 실행 예상 |
| --- | ---: | ---: |
| `ingest-fixtures` | 6시간마다 | 약 120회 |
| `ingest-markets` | 4시간마다 | 약 180회 |
| `run-predictions` | 하루 1회 | 약 30회 |
| `post-match-review` | 6시간마다 + LLM 리뷰 하루 1회 | 약 150회 |
| `report-missing-signal-coverage` | 하루 1회 | 약 30회 |
| 합계 |  | 약 510회 |

월 1500분 목표를 지키려면 정기 워크플로 평균 실행 시간을 약 2.9분 이하로 유지해야 한다. 실제 평균이 이보다 높으면 아래 순서로 조정한다.

## 초과 시 조정 순서

1. `report-missing-signal-coverage`를 주 3회로 줄인다.
2. `post-match-review`의 6시간 주기를 12시간 주기로 줄이고, LLM 리뷰 하루 1회만 유지한다.
3. `ingest-markets`를 6시간 주기로 줄인다.
4. `ingest-fixtures`의 미래 조회 window를 축소하거나 하루 2회로 줄인다.
5. 테스트 워크플로의 중복 workspace 테스트를 정리해 동일한 Vitest suite 반복 실행을 줄인다.

## 변경 전 주의사항

- 경기 직전 odds freshness가 제품 가치에 직접 영향을 주는 시기에는 `ingest-markets` 빈도를 먼저 낮추지 않는다.
- 예측 품질 검증 없이 `run-predictions`를 하루 1회 미만으로 줄이지 않는다.
- LLM advisory 비용과 Actions 분은 별도 예산이므로, LLM 실행 빈도는 API 비용과 함께 검토한다.
- 배포 워크플로는 `test` 성공 후 `main` push에만 이어지므로 정기 배치 예산과 분리해서 본다.
