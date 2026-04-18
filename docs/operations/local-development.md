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

4. `supabase/migrations/202604180001_initial_schema.sql`의 SQL을 적용한다.
5. `supabase/seed.sql`로 초기 데이터를 적재한다.
6. API와 웹 앱 개발 서버를 각각 실행한다.

   각 명령은 별도의 터미널 또는 tmux pane에서 실행한다.

   ```bash
   npm --workspace apps/api run dev
   npm --workspace apps/web run dev
   ```

7. 배치 작업 smoke 실행 또는 샘플 워크플로 검증이 필요하면 아래 환경 변수를 먼저 설정한다.

   ```bash
   export SUPABASE_URL=https://placeholder.supabase.local
    export SUPABASE_PUBLISHABLE_KEY=local-publishable-key
   export R2_BUCKET=workflow-artifacts
   ```

8. GitHub Actions 샘플 워크플로는 실제 운영 비밀값 대신 아래 샘플 전용 설정을 사용한다.

   - `VITE_SUPABASE_URL`
   - `VITE_SUPABASE_PUBLISHABLE_KEY`

   GitHub Actions의 Task 10 샘플 워크플로는 위 값을 읽어 지속형 sample Supabase 프로젝트를 대상으로 실행한다.
   이 sample Supabase 프로젝트에는 미리 현재 저장소의 스키마와 seed 데이터가 적용되어 있어야 한다.
