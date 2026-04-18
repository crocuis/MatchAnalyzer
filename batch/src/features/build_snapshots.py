from batch.src.domain import Checkpoint, MatchSnapshot, SnapshotQuality


def build_snapshot(
    match_id: str,
    checkpoint: Checkpoint,
    lineup_status: str,
    has_market_data: bool,
) -> MatchSnapshot:
    quality = SnapshotQuality.COMPLETE if has_market_data else SnapshotQuality.PARTIAL
    return MatchSnapshot(
        match_id=match_id,
        checkpoint=checkpoint,
        lineup_status=lineup_status,
        quality=quality,
    )
