from __future__ import annotations

import argparse
import json
from pathlib import Path

from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.prediction_dataset import PREDICTION_DATASET_TABLES
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.db_client import DbClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default=".tmp/prediction-dataset",
        help="Directory where table JSON files will be written.",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Table to export. May be repeated. Defaults to prediction experiment tables.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    source_client = DbClient(settings_db_url(settings), settings_db_key(settings))
    target_client = LocalDatasetClient(Path(args.output_dir))
    tables = tuple(args.tables or PREDICTION_DATASET_TABLES)
    row_counts = {}
    for table in tables:
        rows = read_optional_rows(source_client, table)
        row_counts[table] = target_client.write_rows(table, rows)
    print(
        json.dumps(
            {
                "output_dir": str(Path(args.output_dir)),
                "tables": row_counts,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
