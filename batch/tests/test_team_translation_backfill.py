from io import BytesIO
from urllib.error import HTTPError

from batch.src.ingest.fetch_team_translations import (
    build_primary_team_translation_row,
    build_primary_translation_rows_from_mapping,
    build_wikidata_primary_translation_rows,
    cached_request_json,
    filter_missing_primary_translations,
    request_json,
    select_wikidata_candidate,
)


def test_select_wikidata_candidate_prefers_football_club_over_exact_non_football_match():
    team = {"id": "148", "name": "Villarreal", "country": "Spain"}

    selected = select_wikidata_candidate(
        team,
        [
            {
                "id": "Q37452371",
                "label": "Villarreal",
                "description": "family name",
            },
            {
                "id": "Q12297",
                "label": "Villarreal CF",
                "description": "association football club in Villarreal, Spain",
            },
            {
                "id": "Q12292",
                "label": "Villarreal",
                "description": "city in the province of Castellon, Spain",
            },
        ],
    )

    assert selected == {
        "id": "Q12297",
        "label": "Villarreal CF",
        "description": "association football club in Villarreal, Spain",
    }


def test_select_wikidata_candidate_uses_alias_match_for_club_name():
    team = {"id": "108", "name": "Internazionale", "country": "Italy"}

    selected = select_wikidata_candidate(
        team,
        [
            {
                "id": "Q1538737",
                "label": "Internazionale F.C. Torino",
                "description": "association football club",
            },
            {
                "id": "Q3153504",
                "label": "Internazionale",
                "description": "Italian weekly magazine",
            },
            {
                "id": "Q631",
                "label": "Inter Milan",
                "description": "association football club based in Milan, Lombardy, Italy",
                "aliases": ["Internazionale Milano"],
            },
        ],
    )

    assert selected == {
        "id": "Q631",
        "label": "Inter Milan",
        "description": "association football club based in Milan, Lombardy, Italy",
        "aliases": ["Internazionale Milano"],
    }


def test_filter_missing_primary_translations_excludes_existing_ko_primary_rows():
    teams = [
        {"id": "363", "name": "Chelsea"},
        {"id": "382", "name": "Manchester City"},
    ]
    existing_rows = [
        {
            "id": "363:ko:primary",
            "team_id": "363",
            "locale": "ko",
            "display_name": "첼시",
            "is_primary": True,
        },
        {
            "id": "382:ko:betman:맨체스터 시티",
            "team_id": "382",
            "locale": "ko",
            "display_name": "맨체스터 시티",
            "is_primary": False,
        },
    ]

    assert filter_missing_primary_translations(teams, existing_rows, locale="ko") == [
        {"id": "382", "name": "Manchester City"},
    ]


def test_build_primary_team_translation_row_uses_primary_row_shape():
    assert build_primary_team_translation_row(
        team={"id": "363"},
        locale="ko",
        display_name="첼시",
        source_name="wikidata",
    ) == {
        "id": "363:ko:primary",
        "team_id": "363",
        "locale": "ko",
        "display_name": "첼시",
        "source_name": "wikidata",
        "is_primary": True,
    }


def test_build_wikidata_primary_translation_rows_returns_rows_and_misses():
    teams = [
        {"id": "363", "name": "Chelsea", "country": "England"},
        {"id": "382", "name": "Manchester City", "country": "England"},
    ]

    def search_fn(search_term: str):
        if search_term == "Chelsea":
            return [
                {
                    "id": "Q9616",
                    "label": "Chelsea F.C.",
                    "description": "association football club in London, England",
                    "aliases": ["Chelsea"],
                }
            ]
        if search_term == "Manchester City":
            return []
        raise AssertionError(f"unexpected search term: {search_term}")

    def labels_fn(entity_ids: list[str], *, language: str):
        assert entity_ids == ["Q9616"]
        assert language == "ko"
        return {"Q9616": "첼시 FC"}

    rows, misses = build_wikidata_primary_translation_rows(
        teams,
        locale="ko",
        source_name="wikidata",
        search_fn=search_fn,
        labels_fn=labels_fn,
    )

    assert rows == [
        {
            "id": "363:ko:primary",
            "team_id": "363",
            "locale": "ko",
            "display_name": "첼시 FC",
            "source_name": "wikidata",
            "is_primary": True,
        }
    ]
    assert misses == [
        {"id": "382", "name": "Manchester City", "reason": "candidate_not_found"}
    ]


def test_request_json_retries_on_rate_limit():
    attempts = {"count": 0}

    class FakeResponse:
        def __init__(self, payload: bytes):
            self.payload = payload

        def read(self):
            return self.payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def opener(_request, timeout=30):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise HTTPError(
                url="https://example.test",
                code=429,
                msg="Too many requests",
                hdrs=None,
                fp=BytesIO(b""),
            )
        return FakeResponse(b'{\"ok\": true}')

    sleeps: list[float] = []
    payload = request_json(
        "https://example.test",
        opener=opener,
        sleep_fn=sleeps.append,
        throttle_seconds=0.0,
    )

    assert payload == {"ok": True}
    assert attempts["count"] == 2
    assert sleeps == [1.0]


def test_cached_request_json_reuses_cached_payload(tmp_path):
    attempts = {"count": 0}

    class FakeResponse:
        def __init__(self, payload: bytes):
            self.payload = payload

        def read(self):
            return self.payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def opener(_request, timeout=30):
        attempts["count"] += 1
        return FakeResponse(b'{\"ok\": true}')

    first = cached_request_json(
        cache_dir=tmp_path,
        cache_namespace="wikidata-search",
        cache_key="Chelsea",
        request_or_url="https://example.test",
        opener=opener,
        sleep_fn=lambda _seconds: None,
        throttle_seconds=0.0,
    )
    second = cached_request_json(
        cache_dir=tmp_path,
        cache_namespace="wikidata-search",
        cache_key="Chelsea",
        request_or_url="https://example.test",
        opener=opener,
        sleep_fn=lambda _seconds: None,
        throttle_seconds=0.0,
    )

    assert first == {"ok": True}
    assert second == {"ok": True}
    assert attempts["count"] == 1


def test_build_primary_translation_rows_from_mapping_uses_curated_names():
    rows, misses = build_primary_translation_rows_from_mapping(
        teams=[
            {"id": "363", "name": "Chelsea"},
            {"id": "382", "name": "Manchester City"},
        ],
        translation_map={"363": "첼시"},
        locale="ko",
        source_name="curated-ko",
    )

    assert rows == [
        {
            "id": "363:ko:primary",
            "team_id": "363",
            "locale": "ko",
            "display_name": "첼시",
            "source_name": "curated-ko",
            "is_primary": True,
        }
    ]
    assert misses == [
        {"id": "382", "name": "Manchester City", "reason": "mapped_name_not_found"}
    ]
