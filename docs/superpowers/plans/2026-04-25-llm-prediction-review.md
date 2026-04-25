# LLM Prediction Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 기존 경기 예측/리뷰 배치 파이프라인에 NVIDIA Free Endpoint 기반 LLM advisory를 shadow mode로 추가한다.

**Architecture:** `batch/src/llm/`에 설정, 클라이언트, 프롬프트, 검증 경계를 만들고, 예측 job과 리뷰 job은 주입 가능한 함수만 호출한다. LLM 결과는 기존 artifact/summary payload에 저장하며 확률과 추천 본선 값은 변경하지 않는다.

**Tech Stack:** Python 표준 라이브러리 `urllib.request`, Supabase file/REST backend, 기존 batch pytest.

---

### Task 1: LLM 계약 테스트 추가

**Files:**
- Test: `batch/tests/test_llm_advisory.py`

- [ ] 예측 advisory prompt와 fake response normalization 테스트를 추가한다.
- [ ] 리뷰 advisory prompt와 disabled fallback 테스트를 추가한다.
- [ ] `python3 -m pytest batch/tests/test_llm_advisory.py -q`를 실행해 새 모듈이 없어 실패하는 것을 확인한다.

### Task 2: LLM 모듈 구현

**Files:**
- Create: `batch/src/llm/__init__.py`
- Create: `batch/src/llm/advisory.py`
- Modify: `batch/src/settings.py`

- [ ] `Settings`에 LLM 관련 optional field를 추가한다.
- [ ] `.env`, `.env.local`, `batch/.env`, `batch/.env.local`, `batch/env.local`을 모두 지원한다.
- [ ] NVIDIA OpenAI-compatible chat completions 호출 함수를 구현한다.
- [ ] JSON-only system prompt와 schema validator를 구현한다.
- [ ] API 키 없음, flag 꺼짐, 오류 발생 시 compact fallback payload를 반환한다.

### Task 3: 예측 job 연결

**Files:**
- Modify: `batch/src/jobs/run_predictions_job.py`
- Test: `batch/tests/test_reviews.py`

- [ ] `build_prediction_summary_payload`가 `llm_advisory`를 보존하도록 테스트를 추가한다.
- [ ] `main` 루프에서 advisory context를 구성하고 env flag가 켜졌을 때만 LLM을 호출한다.
- [ ] 결과를 `explanation_payload["llm_advisory"]`와 `summary_payload["llm_advisory"]`에 저장한다.

### Task 4: 리뷰 job 연결

**Files:**
- Modify: `batch/src/jobs/run_post_match_review_job.py`
- Test: `batch/tests/test_reviews.py`

- [ ] fake LLM 함수로 review row에 `summary_payload.llm_review`가 추가되는 테스트를 작성한다.
- [ ] 기존 rule-based review 결과를 먼저 만든 뒤 LLM review를 보조 필드로 병합한다.
- [ ] LLM 실패 시 기존 review row 생성이 계속되는지 확인한다.

### Task 5: 검증

**Commands:**
- `python3 -m pytest batch/tests/test_llm_advisory.py batch/tests/test_reviews.py -q`
- `git status --short`
- `git diff --stat`
- `git diff --check`

**Completion Criteria:**
- 새 테스트와 관련 기존 테스트가 통과한다.
- LLM flag가 꺼진 기본 경로는 기존 동작을 유지한다.
- 비밀값은 코드, 문서, 테스트 출력에 포함되지 않는다.
