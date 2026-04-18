import pytest

from batch.src.model.evaluate_walk_forward import split_walk_forward_windows
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
