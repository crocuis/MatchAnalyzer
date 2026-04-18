from pathlib import Path
import sys
from typing import Any


def load_sports_skills_football():
    vendor_path = Path(__file__).resolve().parents[3] / ".vendor"
    if vendor_path.exists() and str(vendor_path) not in sys.path:
        sys.path.insert(0, str(vendor_path))

    from sports_skills import football

    return football


def fetch_daily_schedule(date: str) -> dict[str, Any]:
    football = load_sports_skills_football()
    return football.get_daily_schedule(date=date)


def build_market_snapshots() -> list[dict]:
    return [
        {
            "source_type": "bookmaker",
            "source_name": "sample-book",
            "home_prob": 0.5,
            "draw_prob": 0.25,
            "away_prob": 0.25,
        },
        {
            "source_type": "prediction_market",
            "source_name": "sample-market",
            "home_prob": 0.48,
            "draw_prob": 0.27,
            "away_prob": 0.25,
        },
    ]


def american_odds_to_probability(odds: str) -> float:
    value = int(odds)
    if value > 0:
        return 100 / (value + 100)
    return abs(value) / (abs(value) + 100)


def normalize_market_probabilities(home: float, draw: float, away: float) -> dict[str, float]:
    total = home + draw + away
    return {
        "home_prob": home / total,
        "draw_prob": draw / total,
        "away_prob": away / total,
    }


def build_market_rows_from_schedule(
    schedule: dict[str, Any],
    snapshot_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    snapshot_by_match = {row["match_id"]: row for row in snapshot_rows}
    rows: list[dict[str, Any]] = []

    for event in schedule["data"]["events"]:
        snapshot = snapshot_by_match.get(event["id"])
        odds = event.get("odds") or {}
        moneyline = odds.get("moneyline") or {}
        if not snapshot or not moneyline:
            continue

        normalized = normalize_market_probabilities(
            american_odds_to_probability(moneyline["home"]),
            american_odds_to_probability(moneyline["draw"]),
            american_odds_to_probability(moneyline["away"]),
        )

        rows.append(
            {
                "id": f"{snapshot['id']}_bookmaker",
                "snapshot_id": snapshot["id"],
                "source_type": "bookmaker",
                "source_name": odds.get("provider") or "unknown-bookmaker",
                "home_prob": normalized["home_prob"],
                "draw_prob": normalized["draw_prob"],
                "away_prob": normalized["away_prob"],
                "observed_at": event["start_time"],
            }
        )

    return rows
