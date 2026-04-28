# MatchAnalyzer

MatchAnalyzer는 축구 경기 예측 운영 흐름을 한 저장소에서 관리하는 모노레포다.
현재 저장소에는 다음 범위가 포함되어 있다.

- 경기 목록, 예측 요약, 체크포인트 타임라인, 사후 리뷰를 보여주는 React 대시보드
- Supabase 읽기 모델을 조회하는 Cloudflare Workers API
- 경기 수집, 마켓 수집, 예측 생성, 사후 리뷰 생성 배치 파이프라인
- Supabase 스키마/시드와 GitHub Actions 기반 샘플 자동화

이 저장소는 "실시간 요청 경로에서 무거운 계산을 하지 않고", 배치에서 예측과 리뷰를 만든 뒤 읽기 API와 대시보드에서 소비하는 구조를 기본 원칙으로 둔다.

## 프로젝트 목표

- 경기별 예측 결과를 운영자가 빠르게 확인할 수 있어야 한다.
- 예측이 어떤 체크포인트(`T_MINUS_24H`, `T_MINUS_6H`, `T_MINUS_1H`, `LINEUP_CONFIRMED`)에서 생성되었는지 추적할 수 있어야 한다.
- 경기 종료 후 예측 오차와 시장 비교 결과를 사후 리뷰로 남길 수 있어야 한다.
- 정본 예측 생성은 서버/배치 경로에 두고, 클라이언트는 읽기와 보조 검증에만 집중해야 한다.

## 현재 구성

### `apps/web`

React + Vite 기반 운영 대시보드다.

- 리그 탭별 경기 목록 제공
- 경기 카드에서 추천 픽, 신뢰도, 리뷰 필요 여부 표시
- 경기 상세 모달에서 확률 분포, 체크포인트, 사후 리뷰 표시
- 전체 리포트 뷰 제공
- 한국어/영어 i18n 지원
- 클라이언트 보조 검증 패널은 기본 비활성화

### `apps/api`

Cloudflare Workers + Hono 기반 읽기 API다.

- `GET /health`
- `GET /matches`
- `GET /predictions/:matchId`
- `GET /reviews/:matchId`

Supabase에서 경기, 스냅샷, 예측, 사후 리뷰를 읽어 대시보드가 바로 소비할 수 있는 형태로 반환한다.

### `batch`

Python 기반 배치 파이프라인이다.

- 경기 일정 수집
- 시장 데이터 수집
- 체크포인트별 예측 생성
- 경기 종료 후 사후 리뷰 생성
- 샘플 데이터/스모크 검증 지원

현재 운영 문서 기준으로 자동화는 샘플/스모크 파이프라인 성격이 강하며, 일부 실데이터 연동은 보수적으로 제한되어 있다.

### `supabase`

- 마이그레이션 SQL
- 초기 시드 데이터
- 스키마 통합 테스트

### `packages/contracts`

웹과 API가 공통으로 사용하는 계약 타입과 상태 판별 로직을 둔다.

## 아키텍처 개요

1. 배치가 경기/시장 데이터를 수집한다.
2. 배치가 체크포인트별 스냅샷을 기준으로 예측을 생성한다.
3. 배치가 경기 종료 후 사후 리뷰를 생성한다.
4. API가 Supabase의 읽기 모델을 조회한다.
5. 웹 대시보드가 API를 통해 운영 화면을 렌더링한다.

핵심 원칙:

- 학습과 정본 예측 추론은 요청 경로에서 실행하지 않는다.
- 큰 원천 페이로드는 저장소 밖 오브젝트 스토리지/R2 사용을 우선한다.
- API는 조회용 정규화 결과만 반환한다.
- 클라이언트 계산 결과는 서버 검증 없이는 정본으로 채택하지 않는다.

## 기술 스택

- Web: React 19, Vite, i18next, Vitest, Testing Library
- API: Cloudflare Workers, Hono, Supabase
- Batch: Python 3.12, pytest
- Database: Supabase(Postgres)
- CI/CD: GitHub Actions, Wrangler

## 로컬 실행

기본 기준 버전:

- Node 22
- Python 3.12

의존성 설치:

```bash
npm install
python3 -m pip install -r batch/requirements.txt
```

Supabase 준비:

1. `supabase/migrations/202604180001_initial_schema.sql`을 적용한다.
2. `supabase/seed.sql`로 초기 데이터를 적재한다.

개발 서버 실행:

```bash
npm --workspace apps/api run dev
npm --workspace apps/web run dev
```

또는 루트에서 동시에 실행할 수 있다.

```bash
npm run dev
```

## 환경 변수

### API / 배치 공통

```bash
export SUPABASE_URL=https://your-project.supabase.co
export SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
```

### 선택 사항

배치 산출물 저장 또는 smoke 경로 검증이 필요하면 아래 값을 추가한다.

```bash
export R2_BUCKET=workflow-artifacts
export R2_ACCESS_KEY_ID=your-access-key-id
export R2_SECRET_ACCESS_KEY=your-secret-access-key
export R2_S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com
```

경기 상세 예측/리뷰 응답을 정적 artifact로 우선 제공하려면 배치에서 match-level artifact를 내보낸다.
R2 자격 증명이 없으면 `.tmp/r2/<bucket>`에 로컬 파일로 저장된다.

### 로컬 예측 실험 데이터셋

Supabase egress와 원격 upsert 비용 없이 예측 실험을 반복하려면, 먼저 필요한 읽기 테이블을 로컬 JSON으로 export한다.

```bash
python3 -m batch.src.jobs.export_local_prediction_dataset_job \
  --output-dir .tmp/prediction-dataset
```

이후 예측 생성과 raw signal 평가는 같은 디렉터리를 지정하면 Supabase 대신 로컬 파일을 읽고 쓴다.

```bash
export MATCH_ANALYZER_LOCAL_DATASET_DIR=.tmp/prediction-dataset
export LLM_PREDICTION_ADVISORY_ENABLED=0
export MATCH_ANALYZER_DISABLE_LONG_SIGNAL_REFRESH=1
export MATCH_ANALYZER_FAST_BASELINE_TRAINING=1

REAL_PREDICTION_MATCH_IDS="match_1,match_2" \
  python3 -m batch.src.jobs.run_predictions_job

python3 -m batch.src.jobs.evaluate_raw_prediction_signals_job --all-snapshots
```

로컬 실험 결과는 `.tmp/prediction-dataset/predictions.json`과 `prediction_feature_snapshots.json`에만 반영된다. 운영 DB에 반영하려면 로컬 검증 후 별도 배치 경로로 다시 실행한다.

시즌 전체의 과거 odds 신호를 채울 때는 Odds_API.io historical endpoint를 명시적으로 켠다. 이 경로는 과거 경기의 closing odds를 경기 전 신호로 저장하며, raw payload에 `historical_closing=true`를 남긴다.

```bash
export ODDS_API_IO_INCLUDE_HISTORICAL=1
export ODDS_API_IO_HISTORICAL_ONLY=1
export MATCH_ANALYZER_SKIP_MARKET_ARCHIVE=1
REAL_MARKET_DATE=2026-02-01 python3 -m batch.src.jobs.ingest_markets_job
```

`ODDS_API_IO_INCLUDE_HISTORICAL`을 끄면 기존처럼 현재/미래 경기용 events + multi odds 경로를 사용한다.
`ODDS_API_IO_HISTORICAL_ONLY=1`은 과거 시즌 백필에서 일일 schedule, Betman, Polymarket 호출을 생략해 로컬 실험 시간을 줄인다.
Odds_API.io의 시간당 제한을 아끼고 Football-Data.co.uk bulk CSV만 사용하려면 `ODDS_API_IO_DISABLE=1`을 함께 지정한다.
반복 백필에서는 `FOOTBALL_DATA_CACHE_DIR=.tmp/football-data-cache`를 지정해 리그 CSV를 로컬에 캐시한다.

5대 리그의 시즌 전체 historical closing odds를 한 번에 채우려면 전용 배치를 사용한다. 이 배치는 `market_probabilities`와 `market_variants`만 upsert하며, Champions/Europa/Conference League처럼 Football-Data CSV가 없는 대회는 건드리지 않는다.

```bash
export MATCH_ANALYZER_LOCAL_DATASET_DIR=.tmp/prediction-dataset
export FOOTBALL_DATA_CACHE_DIR=.tmp/football-data-cache
FOOTBALL_DATA_BACKFILL_START_DATE=2025-12-01 \
  python3 -m batch.src.jobs.backfill_football_data_markets_job
```

유럽 대항전처럼 Football-Data CSV가 없는 대회는 Odds_API.io historical closing odds를 rate-limit 친화적인 캐시형 배치로 채운다. 이 배치는 기본적으로 Champions/Europa/Conference League만 대상으로 하고, `ODDS_API_IO_HISTORICAL_MAX_REQUESTS_PER_RUN`에 도달하면 정상 종료한다. 같은 캐시 디렉터리로 다시 실행하면 이미 받은 events/odds를 재사용해 이어서 채운다.

```bash
export MATCH_ANALYZER_LOCAL_DATASET_DIR=.tmp/prediction-dataset
export ODDS_API_IO_HISTORICAL_CACHE_DIR=.tmp/odds-api-io-historical-cache
export ODDS_API_IO_HISTORICAL_MAX_REQUESTS_PER_RUN=80
ODDS_API_IO_HISTORICAL_START_DATE=2025-12-01 \
  python3 -m batch.src.jobs.backfill_odds_api_io_historical_markets_job
```

```bash
python3 -m batch.src.jobs.export_match_artifacts_job
```

데일리 픽 목록도 날짜별 artifact로 내보낼 수 있다. 이 경로는 `daily_pick_runs`, `daily_pick_items`,
`daily_pick_results`, `daily_pick_performance_summary`를 읽어 `/daily-picks?date=...` 응답을 미리 만든다.

```bash
DAILY_PICK_ARTIFACT_DATE=2026-04-24 python3 -m batch.src.jobs.export_daily_pick_artifacts_job
```

로컬 Worker가 이 artifact를 fetch하도록 테스트하려면 정적 서버를 띄우고 API env에 base URL을 지정한다.

```bash
python3 -m http.server 8788 --directory .tmp/r2/workflow-artifacts
export MATCH_ANALYZER_ARTIFACT_BASE_URL=http://localhost:8788
```

운영에서는 R2 공개/서명 URL을 `stored_artifacts.storage_uri`에 저장하거나, Worker가 접근 가능한 artifact base URL을
`MATCH_ANALYZER_ARTIFACT_BASE_URL`로 제공한다.

웹 앱에서 API 원점을 명시하려면 아래 값을 사용한다.

```bash
export VITE_API_BASE_URL=https://your-api-origin.example.com
```

운영 리포트 API를 보호하려면 API Worker와 Pages Function 런타임에 같은 키를 설정한다.
웹 대시보드는 민감 리포트 요청만 같은 origin의 `/api/...` 프록시로 보내며, Pages `_worker.js`가 서버 측에서 이 키를 주입한다.

```bash
export OPERATIONAL_REPORTS_API_KEY=your-operational-report-key
export MATCH_ANALYZER_API_ORIGIN=https://your-api-origin.example.com
```

## 테스트

저장소 규칙상 아래 순서로 검증한다.

```bash
npm test
npm --workspace apps/api run test
npm --workspace apps/web run test
python3 -m pytest
```

## 저장소 구조

```text
.
├── apps/
│   ├── api/          # Cloudflare Workers 읽기 API
│   └── web/          # React 운영 대시보드
├── batch/            # Python 배치 파이프라인
├── packages/
│   └── contracts/    # 공통 타입/계약
├── supabase/         # 마이그레이션, 시드, 스키마 테스트
├── docs/             # 운영 문서와 설계 메모
└── .github/workflows/# CI/CD 및 샘플 자동화
```

## 운영 메모

- 체크포인트 기본 정책은 `T-24H`, `T-6H`, `T-1H`, `LINEUP_CONFIRMED`다.
- 클라이언트 보조 검증 기능은 operator-only이며 기본 비활성화 상태다.
- 성능 예산은 `/health`, `/matches`, `/predictions/:matchId` 및 각 배치 작업별로 따로 정의되어 있다.
- 현재 GitHub Actions 자동화는 샘플/스모크 검증 성격이 강하다.

## 관련 문서

- [로컬 개발 가이드](docs/operations/local-development.md)
- [체크포인트 스케줄링](docs/operations/checkpoint-scheduling.md)
- [성능 예산](docs/operations/performance-budget.md)
- [Client-assisted validation 정책](docs/operations/client-assisted-validation.md)
