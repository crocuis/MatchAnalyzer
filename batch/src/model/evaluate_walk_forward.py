def split_walk_forward_windows(
    seasons: list[str], minimum_train_size: int
) -> list[tuple[list[str], list[str]]]:
    if minimum_train_size < 1:
        raise ValueError("minimum_train_size must be at least 1")
    if minimum_train_size >= len(seasons):
        raise ValueError("minimum_train_size must be smaller than the number of seasons")

    windows: list[tuple[list[str], list[str]]] = []
    for index in range(minimum_train_size, len(seasons)):
        train = seasons[:index]
        test = [seasons[index]]
        windows.append((train, test))
    return windows
