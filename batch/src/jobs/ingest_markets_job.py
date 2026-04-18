import json

from batch.src.ingest.fetch_markets import build_market_snapshot


def main() -> None:
    print(json.dumps(build_market_snapshot(), sort_keys=True))


if __name__ == "__main__":
    main()
