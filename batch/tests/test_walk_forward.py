import pytest
import warnings

from batch.src.model.evaluate_walk_forward import (
    calibrate_confidence_from_buckets,
    split_walk_forward_windows,
    summarize_confidence_buckets,
)
from batch.src.model.train_baseline import train_baseline_model


def test_split_walk_forward_windows_preserves_time_order():
    seasons = ["2022", "2023", "2024", "2025"]

    windows = split_walk_forward_windows(seasons, minimum_train_size=2)

    assert windows == [
        (["2022", "2023"], ["2024"]),
        (["2022", "2023", "2024"], ["2025"]),
    ]


def test_split_walk_forward_windows_rejects_minimum_train_size_below_one():
    seasons = ["2022", "2023", "2024"]

    with pytest.raises(ValueError, match="minimum_train_size must be at least 1"):
        split_walk_forward_windows(seasons, minimum_train_size=0)


def test_split_walk_forward_windows_rejects_minimum_train_size_without_test_window():
    seasons = ["2022", "2023", "2024"]

    with pytest.raises(
        ValueError,
        match="minimum_train_size must be smaller than the number of seasons",
    ):
        split_walk_forward_windows(seasons, minimum_train_size=len(seasons))


def test_train_baseline_model_exposes_predict_proba():
    features = [[0.0], [0.1], [0.2], [1.0], [1.1], [1.2]]
    labels = [0, 0, 0, 1, 1, 1]

    model = train_baseline_model(features, labels)

    assert hasattr(model, "predict_proba")
    probabilities = model.predict_proba([[0.15], [1.15]])
    assert probabilities.shape == (2, 2)


def test_train_baseline_model_rejects_classes_with_fewer_than_three_samples():
    features = [[0.0], [0.1], [0.2], [1.0], [1.1]]
    labels = [0, 0, 0, 1, 1]

    with pytest.raises(
        ValueError,
        match="train_baseline_model requires at least 3 samples per class for calibration",
    ):
        train_baseline_model(features, labels)


def test_train_baseline_model_records_candidate_comparison_metadata():
    features = [
        [0.0, 0.0],
        [0.1, 0.1],
        [0.2, 0.2],
        [0.15, 0.05],
        [1.0, 1.0],
        [1.1, 1.1],
        [1.2, 1.2],
        [1.05, 0.95],
        [2.0, 2.0],
        [2.1, 2.1],
        [2.2, 2.2],
        [2.05, 1.95],
    ]
    labels = [
        "HOME",
        "HOME",
        "HOME",
        "HOME",
        "DRAW",
        "DRAW",
        "DRAW",
        "DRAW",
        "AWAY",
        "AWAY",
        "AWAY",
        "AWAY",
    ]

    model = train_baseline_model(features, labels)

    assert model.selected_candidate_ == "logistic_regression"
    assert model.selection_metadata_["selected_candidate"] == model.selected_candidate_
    assert model.selection_metadata_["selection_metric"] == "neg_log_loss"
    assert model.selection_metadata_["selection_ran"] is True
    assert set(model.selection_metadata_["candidate_scores"]) == {"logistic_regression"}
    assert all(
        isinstance(score, float)
        for score in model.selection_metadata_["candidate_scores"].values()
    )


def test_train_baseline_model_emits_no_deprecation_warning():
    features = [
        [0.0, 0.0],
        [0.1, 0.1],
        [0.2, 0.2],
        [0.15, 0.05],
        [1.0, 1.0],
        [1.1, 1.1],
        [1.2, 1.2],
        [1.05, 0.95],
        [2.0, 2.0],
        [2.1, 2.1],
        [2.2, 2.2],
        [2.05, 1.95],
    ]
    labels = [
        "HOME",
        "HOME",
        "HOME",
        "HOME",
        "DRAW",
        "DRAW",
        "DRAW",
        "DRAW",
        "AWAY",
        "AWAY",
        "AWAY",
        "AWAY",
    ]

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        train_baseline_model(features, labels)


def test_summarize_confidence_buckets_groups_hits_by_bucket():
    summary = summarize_confidence_buckets(
        [
            {"confidence": 0.81, "is_correct": True},
            {"confidence": 0.86, "is_correct": False},
            {"confidence": 0.64, "is_correct": True},
            {"confidence": 0.67, "is_correct": True},
        ]
    )

    assert summary["0.8-0.9"]["count"] == 2
    assert summary["0.8-0.9"]["hit_rate"] == 0.5
    assert summary["0.6-0.7"]["count"] == 2
    assert summary["0.6-0.7"]["hit_rate"] == 1.0


def test_calibrate_confidence_from_buckets_blends_raw_score_with_history():
    summary = {
        "0.8-0.9": {"count": 5, "hit_rate": 0.6},
        "0.6-0.7": {"count": 4, "hit_rate": 0.75},
    }

    assert calibrate_confidence_from_buckets(0.83, summary, minimum_count=3) == 0.715
    assert (
        calibrate_confidence_from_buckets(
            0.83,
            summary,
            minimum_count=3,
            maximum_calibration_gap=0.08,
        )
        == 0.68
    )
    assert calibrate_confidence_from_buckets(0.52, summary, minimum_count=3) == 0.52
