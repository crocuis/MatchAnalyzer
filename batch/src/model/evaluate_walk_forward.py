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


def confidence_bucket_label(confidence: float, bucket_size: float = 0.1) -> str:
    bounded = min(max(confidence, 0.0), 1.0)
    lower = min(int(bounded / bucket_size) * bucket_size, 1.0 - bucket_size)
    upper = min(lower + bucket_size, 1.0)
    return f"{lower:.1f}-{upper:.1f}"


def summarize_confidence_buckets(
    records: list[dict], bucket_size: float = 0.1
) -> dict[str, dict[str, float | int]]:
    grouped: dict[str, dict[str, float | int]] = {}
    for record in records:
        bucket = confidence_bucket_label(float(record["confidence"]), bucket_size=bucket_size)
        current = grouped.setdefault(bucket, {"count": 0, "hits": 0})
        current["count"] += 1
        current["hits"] += 1 if record["is_correct"] else 0

    return {
        bucket: {
            "count": int(values["count"]),
            "hit_rate": round(int(values["hits"]) / int(values["count"]), 3),
        }
        for bucket, values in grouped.items()
    }


def calibrate_confidence_from_buckets(
    raw_confidence: float,
    bucket_summary: dict[str, dict[str, float | int]],
    minimum_count: int = 3,
    maximum_calibration_gap: float | None = None,
) -> float:
    bucket = confidence_bucket_label(raw_confidence)
    summary = bucket_summary.get(bucket)
    if not summary or int(summary["count"]) < minimum_count:
        return round(raw_confidence, 4)

    hit_rate = float(summary["hit_rate"])
    calibrated = (raw_confidence * 0.5) + (hit_rate * 0.5)
    if maximum_calibration_gap is not None:
        calibrated = min(calibrated, hit_rate + maximum_calibration_gap)
    return round(min(max(calibrated, 0.0), 1.0), 4)
