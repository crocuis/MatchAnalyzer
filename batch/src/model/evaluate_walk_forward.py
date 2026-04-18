def split_walk_forward_windows(
    seasons: list[str], minimum_train_size: int
) -> list[tuple[list[str], list[str]]]:
    windows: list[tuple[list[str], list[str]]] = []
    for index in range(minimum_train_size, len(seasons)):
        train = seasons[:index]
        test = [seasons[index]]
        windows.append((train, test))
    return windows
