from dataclasses import dataclass
from enum import Enum
from typing import Literal


Checkpoint = Literal[
    "T_MINUS_24H",
    "T_MINUS_6H",
    "T_MINUS_1H",
    "LINEUP_CONFIRMED",
]
CHECKPOINTS: tuple[Checkpoint, ...] = (
    "T_MINUS_24H",
    "T_MINUS_6H",
    "T_MINUS_1H",
    "LINEUP_CONFIRMED",
)


class SnapshotQuality(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"


@dataclass(slots=True, frozen=True)
class MatchSnapshot:
    match_id: str
    checkpoint: Checkpoint
    lineup_status: str
    quality: SnapshotQuality
    captured_at: str | None = None

    def __post_init__(self) -> None:
        if self.checkpoint not in CHECKPOINTS:
            raise ValueError(f"checkpoint must be one of {CHECKPOINTS}")

        try:
            object.__setattr__(self, "quality", SnapshotQuality(self.quality))
        except ValueError as exc:
            raise ValueError("quality must be a valid SnapshotQuality") from exc
