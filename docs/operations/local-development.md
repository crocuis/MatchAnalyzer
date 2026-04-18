# 로컬 개발

로컬 개발 환경은 Node 22와 Python 3.12를 기준으로 맞춘다.

1. Node 22와 Python 3.12를 설치한다.
2. 다음 명령으로 Node 의존성을 설치한다.

   ```bash
   npm install
   ```

3. 다음 명령으로 배치 작업용 Python 의존성을 설치한다.

   ```bash
   pip install -r batch/requirements.txt
   ```

4. `supabase/migrations/202604180001_initial_schema.sql`의 SQL을 적용한다.
5. `supabase/seed.sql`로 초기 데이터를 적재한다.
6. API와 웹 앱 개발 서버를 각각 실행한다.

   ```bash
   npm --workspace apps/api run dev
   npm --workspace apps/web run dev
   ```
