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

웹 앱에서 API 원점을 명시하려면 아래 값을 사용한다.

```bash
export VITE_API_BASE_URL=https://your-api-origin.example.com
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
