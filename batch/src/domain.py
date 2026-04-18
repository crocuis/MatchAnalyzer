from dataclasses import dataclass
from enum import Enum


class SnapshotQuality(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"


@dataclass(slots=True)
class MatchSnapshot:
    match_id: str
    checkpoint: str
    lineup_status: str
    quality: SnapshotQuality
