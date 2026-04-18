from batch.src.model.evaluate_walk_forward import split_walk_forward_windows


def test_split_walk_forward_windows_preserves_time_order():
    seasons = ["2022", "2023", "2024", "2025"]

    windows = split_walk_forward_windows(seasons, minimum_train_size=2)

    assert windows == [
        (["2022", "2023"], ["2024"]),
        (["2022", "2023", "2024"], ["2025"]),
    ]
