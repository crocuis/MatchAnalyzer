from __future__ import annotations


CENTROID_DEPLOYABILITY_HOLD_REASON = "unvalidated_centroid_fallback"
DEPLOYABILITY_EXCLUDED_BASE_MODEL_SOURCES = {
    "centroid_fallback",
    "centroid_poisson_blend",
}


def is_deployable_base_model_source(base_model_source: object) -> bool:
    return str(base_model_source or "") not in DEPLOYABILITY_EXCLUDED_BASE_MODEL_SOURCES


def build_model_source_deployment_gate(base_model_source: object) -> dict | None:
    source = str(base_model_source or "")
    if is_deployable_base_model_source(source):
        return None
    return {
        "recommended": False,
        "reason": CENTROID_DEPLOYABILITY_HOLD_REASON,
        "base_model_source": source,
    }
