from collections import Counter

from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


SELECTION_METRIC = "neg_log_loss"
CALIBRATION_FOLDS = 3


def build_baseline_candidate_estimators() -> dict[str, object]:
    return {
        "logistic_regression": make_pipeline(
            StandardScaler(),
            LogisticRegression(
                solver="saga",
                max_iter=5000,
                random_state=7,
            ),
        ),
        "hist_gradient_boosting": HistGradientBoostingClassifier(random_state=7),
    }


def select_baseline_candidate(features, labels) -> tuple[str, dict[str, float]]:
    class_counts = Counter(labels)
    fold_count = min(CALIBRATION_FOLDS, min(class_counts.values()))
    splitter = StratifiedKFold(n_splits=fold_count, shuffle=True, random_state=7)
    scores: dict[str, float] = {}

    for name, estimator in build_baseline_candidate_estimators().items():
        candidate_scores = cross_val_score(
            estimator,
            features,
            labels,
            cv=splitter,
            scoring=SELECTION_METRIC,
            error_score="raise",
        )
        scores[name] = float(candidate_scores.mean())

    selected_candidate = max(scores, key=scores.get)
    return selected_candidate, scores


def train_baseline_model(features, labels):
    class_counts = Counter(labels)
    if any(count < CALIBRATION_FOLDS for count in class_counts.values()):
        raise ValueError(
            "train_baseline_model requires at least 3 samples per class for isotonic calibration"
        )

    selected_candidate, candidate_scores = select_baseline_candidate(features, labels)
    estimator = build_baseline_candidate_estimators()[selected_candidate]
    model = CalibratedClassifierCV(estimator, method="isotonic", cv=CALIBRATION_FOLDS)
    model.fit(features, labels)
    model.selected_candidate_ = selected_candidate
    model.selection_metadata_ = {
        "selected_candidate": selected_candidate,
        "selection_metric": SELECTION_METRIC,
        "selection_ran": True,
        "candidate_scores": {
            name: round(score, 6) for name, score in sorted(candidate_scores.items())
        },
    }
    return model
