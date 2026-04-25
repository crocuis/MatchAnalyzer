# LLM 예측/리뷰 보조 레이어 설계

## 목표

모든 경기 예측 파이프라인에 LLM을 직접 확률 산출기로 넣지 않고, 기존 수치 모델과 시장 기반 확률 위에 shadow advisory를 추가한다. 1차 목표는 예측 적중률을 즉시 바꾸는 것이 아니라, 비정형 리스크와 경기 후 실패 원인을 구조화해서 이후 보정 정책과 피처 개선에 쓸 수 있게 만드는 것이다.

## 원칙

- 기존 `home_prob`, `draw_prob`, `away_prob`, `recommended_pick`, `confidence_score`는 LLM 결과로 변경하지 않는다.
- LLM 결과는 `explanation_payload`와 `summary_payload`의 보조 필드로 저장한다.
- 기본 모델은 `deepseek-ai/deepseek-v4-flash`, 고위험 재검토 모델은 이후 `deepseek-ai/deepseek-v4-pro`로 확장할 수 있게 provider/model 설정을 분리한다.
- 네트워크 호출은 환경 변수로 명시적으로 켠 경우에만 수행한다.
- LLM 출력은 JSON만 허용하고, 파싱 실패나 형식 불일치가 있으면 예측/리뷰 본선 파이프라인을 실패시키지 않고 `status=unavailable`로 남긴다.

## 예측 단계 입출력

예측 단계는 경기 메타데이터, 세 원천 확률, 추천/신뢰도, feature context, source metadata를 LLM에 전달한다. LLM은 확률을 재계산하지 않고 제한된 보조 판단만 반환한다.

```json
{
  "schema_version": "prediction_llm_advisory.v1",
  "status": "available",
  "provider": "nvidia",
  "model": "deepseek-ai/deepseek-v4-flash",
  "risk_flags": ["lineup_uncertainty", "market_model_divergence"],
  "context_adjustment": {
    "home": -0.02,
    "draw": 0.01,
    "away": 0.01
  },
  "confidence_modifier": -0.06,
  "recommended_action": "keep_pick",
  "reason_codes": ["home_short_rest"],
  "analyst_summary": "Home remains the model lean, but short rest reduces conviction.",
  "evidence_limits": ["confirmed_lineups_unavailable"]
}
```

`context_adjustment`는 이번 단계에서 저장만 한다. 후속 rollout에서 검증된 뒤에만 확률 보정에 사용한다.

## 리뷰 단계 입출력

리뷰 단계는 기존 rule-based review 결과, 실제 결과, 예측 확률, 시장 비교, 기존 taxonomy를 전달한다. LLM은 사람이 읽을 수 있는 실패 원인과 개선 후보를 구조화한다.

```json
{
  "schema_version": "post_match_llm_review.v1",
  "status": "available",
  "provider": "nvidia",
  "model": "deepseek-ai/deepseek-v4-flash",
  "miss_reason_family": "lineup_or_availability",
  "severity": "medium",
  "model_blindspots": ["lineup_strength_delta_underweighted"],
  "data_gaps": ["confirmed_lineups_unavailable"],
  "actionable_fixes": [
    "increase review priority when lineup_status is projected and confidence exceeds 0.65"
  ],
  "should_change_features": true,
  "review_summary": "The miss appears driven by lineup uncertainty rather than base probability failure."
}
```

## 확장 지점

- `batch/src/llm/`에 provider-agnostic client, JSON validation, prompt builder를 둔다.
- `batch/src/settings.py`는 `NVIDIA_API_KEY`, `NVIDIA_BASE_URL`, `LLM_PREDICTION_MODEL`, `LLM_REVIEW_MODEL`을 읽는다.
- `batch/src/jobs/run_predictions_job.py`는 `LLM_PREDICTION_ADVISORY_ENABLED=1`일 때만 advisory를 생성한다.
- `batch/src/jobs/run_post_match_review_job.py`는 `LLM_REVIEW_ADVISORY_ENABLED=1`일 때만 review advisory를 생성한다.
- 저장은 기존 artifact/summary payload 필드를 재사용한다. 새 DB 컬럼은 만들지 않는다.

## 실패 처리

- API 키가 없거나 flag가 꺼져 있으면 `status=disabled`를 반환한다.
- API 오류, JSON 파싱 오류, 스키마 오류는 `status=unavailable`과 `error_code`로 축약 저장한다.
- LLM 실패는 예측/리뷰 생성 실패로 전파하지 않는다.

## 테스트 전략

- LLM prompt builder가 필요한 입력만 담고 민감한 환경값을 포함하지 않는지 테스트한다.
- fake LLM client를 주입해 예측 payload에 `llm_advisory`가 저장되는지 테스트한다.
- fake LLM client를 주입해 post-match review `summary_payload.llm_review`가 저장되는지 테스트한다.
- disabled 상태에서는 기존 payload와 확률이 바뀌지 않는지 테스트한다.
