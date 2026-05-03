from __future__ import annotations

import argparse
import json

from batch.src.model.betting_recommendations import (
    evaluate_settled_betting_recommendations,
    zero_metrics,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.db_client import DbClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    _args = parse_args(argv)
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    matches = read_optional_rows(client, "matches")
    snapshots = read_optional_rows(client, "match_snapshots")
    predictions = read_optional_rows(client, "predictions")
    teams = read_optional_rows(client, "teams")
    variant_rows = read_optional_rows(client, "market_variants")

    if not matches or not snapshots or not predictions:
        print(json.dumps(zero_metrics(), sort_keys=True))
        return

    payload = evaluate_settled_betting_recommendations(
        matches=matches,
        snapshots=snapshots,
        predictions=predictions,
        variant_rows=variant_rows,
        teams=teams,
    )
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
