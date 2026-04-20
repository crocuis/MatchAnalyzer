import json
import os
from datetime import date

from batch.src.jobs.backfill_assets_job import iter_dates
from batch.src.jobs.run_post_match_review_job import run_review_job
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    start = os.environ.get("REVIEW_BACKFILL_START")
    end = os.environ.get("REVIEW_BACKFILL_END")
    if not start or not end:
        raise KeyError("REVIEW_BACKFILL_START and REVIEW_BACKFILL_END")

    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)

    dates = iter_dates(date.fromisoformat(start), date.fromisoformat(end))
    results = [run_review_job(client, target_date=day) for day in dates]

    print(
        json.dumps(
            {
                "date_start": start,
                "date_end": end,
                "date_count": len(dates),
                "inserted_total": sum(int(result.get("inserted_rows") or 0) for result in results),
                "reviewed_match_total": sum(int(result.get("result_rows") or 0) for result in results),
                "skip_reason_counts": {
                    reason: sum(
                        1
                        for result in results
                        if result.get("skip_reason") == reason
                    )
                    for reason in sorted(
                        {
                            result.get("skip_reason")
                            for result in results
                            if result.get("skip_reason")
                        }
                    )
                },
                "results": results,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
