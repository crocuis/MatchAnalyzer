from dataclasses import dataclass
from enum import Enum
from typing import Literal


Checkpoint = Literal[
    "T_MINUS_24H",
    "T_MINUS_6H",
    "T_MINUS_1H",
    "LINEUP_CONFIRMED",
]


class SnapshotQuality(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"


@dataclass(slots=True)
class MatchSnapshot:
    match_id: str
    checkpoint: Checkpoint
    lineup_status: str
    quality: SnapshotQuality
