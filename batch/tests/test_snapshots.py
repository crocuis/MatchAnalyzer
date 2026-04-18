from batch.src.domain import MatchSnapshot, SnapshotQuality


def test_snapshot_requires_checkpoint_and_quality():
    snapshot = MatchSnapshot(
        match_id="match_001",
        checkpoint="T_MINUS_24H",
        lineup_status="unknown",
        quality=SnapshotQuality.COMPLETE,
    )

    assert snapshot.checkpoint == "T_MINUS_24H"
    assert snapshot.quality.value == "complete"
