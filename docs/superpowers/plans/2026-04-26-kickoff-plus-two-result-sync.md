# kickoff +2H 결과 동기화 구현 계획

## 목표

경기 시작 2시간 후에도 미정산 상태인 경기만 좁게 재조회하는 결과 동기화 경로를 만든다.

## 구조

새 Python job이 DB의 미정산 후보를 고르고 날짜별 schedule을 조회해 대상 경기 결과만 upsert한다. 새 GitHub Actions workflow가 이 job을 2시간마다 실행하고, 결과가 새로 들어온 날짜만 후속 review/settle/export로 넘긴다.

## 기술 스택

Python batch jobs, Supabase REST client wrapper, 기존 fixture normalizer, GitHub Actions YAML, pytest를 사용한다.

## 작업

- [x] `batch/src/jobs/sync_match_results_job.py`에 미정산 후보 선별, 날짜별 결과 조회, 결과 upsert, JSON 출력 로직을 추가한다.
- [x] `batch/tests/test_sync_match_results_job.py`에 후보 필터, date별 fetch, 결과 upsert, main 출력 테스트를 추가한다.
- [x] `.github/workflows/sync-match-results.yml`에 2시간 주기 결과 동기화 workflow를 추가한다.
- [x] `batch/tests/test_workflows.py`에 result sync workflow의 cron, env, 후속 review/settle/export 조건 테스트를 추가한다.
- [x] YAML parse, `git diff --check`, Node/Python 전체 테스트로 검증한다.

## 검증 명령

- `ruby -e "require 'yaml'; Dir['.github/workflows/*.yml'].sort.each { |path| YAML.load_file(path); puts path }"`
- `git status --short`
- `git diff --stat`
- `git diff --check`
- `npm test`
- `npm --workspace apps/api run test`
- `npm --workspace apps/web run test`
- `python3 -m pytest`
