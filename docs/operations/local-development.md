# 로컬 개발

로컬 개발 환경은 Node 22와 Python 3.12를 기준으로 맞춘다.

1. Node 22와 Python 3.12를 설치한다.
2. 다음 명령으로 Node 의존성을 설치한다.

   ```bash
   npm install
   ```

3. 다음 명령으로 배치 작업용 Python 의존성을 설치한다.

   ```bash
   python3 -m pip install -r batch/requirements.txt
   ```

4. Neon 개발 브랜치의 연결 문자열을 `.env.local`, `apps/api/.dev.vars`, `batch/.env.local`에 `DATABASE_URL`로 저장한다.
   URL에는 `&channel_binding=require` 같은 쿼리 문자열이 들어가므로 작은따옴표로 감싼다.
5. 새 로컬 DB를 직접 준비해야 할 때만 `supabase/migrations/202604180001_initial_schema.sql`의 SQL과 `supabase/seed.sql`을 적용한다.
6. API와 웹 앱 개발 서버를 각각 실행한다.

   각 명령은 별도의 터미널 또는 tmux pane에서 실행한다.

   ```bash
   npm --workspace apps/api run dev
   npm --workspace apps/web run dev
   ```

7. 배치 작업 smoke 실행 또는 샘플 워크플로 검증이 필요하면 아래 환경 변수를 먼저 설정한다.

   ```bash
   export DATABASE_URL='postgresql://user:password@host/neondb?sslmode=require&channel_binding=require'
   export R2_BUCKET=workflow-artifacts
   ```

   또는 `batch/.env.local` 파일을 만들어 같은 값을 저장해도 된다. 템플릿은 `batch/.env.example`을 기준으로 맞춘다.

8. GitHub Actions와 배포 환경은 Neon 연결 문자열을 GitHub secret `NEON_DATABASE_URL` 또는 `DATABASE_URL`로 받는다.
   값은 `postgresql://user:password@host/db?...` 형태의 전체 연결 문자열이어야 하며, Neon 비밀번호에 `#`, `@`, `:` 같은 특수문자가 있으면 URL 인코딩된 값을 저장한다.
   운영 배포에는 아래 secret/variable을 설정한다.

   - `NEON_DATABASE_URL` 또는 `DATABASE_URL`
   - `CLOUDFLARE_API_TOKEN`
   - `CLOUDFLARE_ACCOUNT_ID`
   - `CLOUDFLARE_PAGES_PROJECT_NAME`
   - `VITE_API_BASE_URL`
   - `OPERATIONAL_REPORTS_API_KEY`
   - `MATCH_ANALYZER_ARTIFACT_BASE_URL` (선택 variable)

   `deploy-production`은 Supabase CLI 대신 `scripts/apply_postgres_migrations.py`를 실행한다.
   이 스크립트는 `supabase/migrations/*.sql`을 Postgres에 파일명 순서대로 적용하고 `public.match_analyzer_schema_migrations`에 적용 이력을 기록한다.
   이미 복원된 DB에 ledger가 없으면 자동으로 모든 migration을 적용 완료 처리하지 않는다.
   복원본의 마지막 적용 migration을 확인한 경우에만 `MATCH_ANALYZER_MIGRATION_BASELINE_VERSION` variable을 설정해 그 version까지 baseline 처리한다.
   `DATABASE_URL`을 Cloudflare Worker secret으로 주입하기 전에 migration과 Neon DB smoke check를 수행하고, 배포 후 API endpoint smoke check를 수행한다.

9. Cloudflare R2를 실제로 연결할 경우 아래 값을 추가한다.

   ```bash
   export R2_ACCESS_KEY_ID=your-access-key-id
   export R2_SECRET_ACCESS_KEY=your-secret-access-key
   export R2_S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com
   ```

   이 값들이 없으면 현재 배치 smoke 경로는 `.tmp/r2/` 파일 저장 fallback을 사용한다.

10. Polymarket 실데이터를 함께 검증할 때는 아래 순서를 권장한다.

   ```bash
   REAL_FIXTURE_DATE=2026-04-27 python3 -m batch.src.jobs.ingest_fixtures_job
   REAL_MARKET_DATE=2026-04-27 python3 -m batch.src.jobs.ingest_markets_job
   REAL_PREDICTION_DATE=2026-04-27 python3 -m batch.src.jobs.run_predictions_job
   REAL_REVIEW_DATE=2026-04-12 python3 -m batch.src.jobs.run_post_match_review_job
   ```

   참고:
   - GitHub Actions의 fixtures ingestion은 기준 UTC 날짜와 `+7일..+14일` 구간을 순회한다. 가까운 1주 선행 적재는 필요할 때 로컬에서 `REAL_FIXTURE_DATE`를 날짜별로 지정해 실행한다.
   - `REAL_FIXTURE_HYDRATE_HISTORY=1`을 추가하면 fixture ingest 중 팀 일정 기반 historical metric 보강을 수행한다. 선행 적재 기본 경로에서는 기존 `matches`만 사용해 원격 호출량을 줄인다.
   - `REAL_FIXTURE_BACKFILL_TEAM_ASSETS=1`을 추가하면 fixture ingest 중 누락 팀 로고 fallback 조회를 수행한다. 기본 경로는 원천 schedule에 포함된 asset과 기존 DB asset만 병합한다.
   - 라인업/최근 선수 form 동기화는 킥오프 1시간 이내 경기만 대상으로 한다. 그보다 먼 예정 경기는 일정과 기본 스냅샷만 먼저 적재한다.
   - Polymarket 연동은 현재 `epl`, `ucl`, `uel`, `kor` 계열만 보수적으로 지원한다.
   - `prediction_market` row가 없어도 prediction/review는 bookmaker fallback으로 계속 진행된다.
   - `match_snapshots.snapshot_quality`는 시장 row가 붙으면 `complete`로 승격된다.
