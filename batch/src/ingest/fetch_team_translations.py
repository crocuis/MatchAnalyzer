from __future__ import annotations

import json
import time
from hashlib import sha1
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_USER_AGENT = "MatchAnalyzerBot/1.0 (team translation backfill)"
WIKIDATA_THROTTLE_SECONDS = 0.2
WIKIDATA_CACHE_DIR = Path(".tmp") / "team_translation_cache"
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
FOOTBALL_DESCRIPTION_KEYWORDS = (
    "association football",
    "football club",
    "football team",
    "professional football club",
    "national football team",
    "soccer club",
    "soccer team",
    "sports club",
)
NON_TEAM_DESCRIPTION_KEYWORDS = (
    "women",
    "woman",
    "female",
    "basketball",
    "disambiguation",
    "family name",
    "city",
    "province",
    "region",
    "municipality",
    "opera",
    "magazine",
    "album",
    "tram stop",
    "company",
    "book",
    "ship",
    "asteroid",
    "journal",
    "wikimedia",
    "category",
    "season of football team",
    "player",
)
TEAM_STOP_WORDS = (
    "football club",
    "football team",
    "club de futbol",
    "fc",
    "cf",
    "ac",
    "afc",
    "as",
    "ssc",
    "ss",
    "fk",
    "sk",
    "if",
    "bk",
    "sc",
    "club",
    "calcio",
    "sporting",
    "sport",
)


def normalize_lookup_name(value: object) -> str:
    text = str(value or "").lower().replace("&", " and ")
    cleaned = "".join(
        character if character.isalnum() or character.isspace() else " "
        for character in text
    )
    return " ".join(cleaned.split())


def strip_team_stop_words(value: object) -> str:
    normalized = normalize_lookup_name(value)
    for stop_word in TEAM_STOP_WORDS:
        normalized = normalized.replace(stop_word, " ")
    return " ".join(normalized.split())


def _candidate_score(team: dict[str, Any], candidate: dict[str, Any]) -> int:
    description = str(candidate.get("description") or "").lower()
    label = candidate.get("label")
    aliases = candidate.get("aliases") or []
    normalized_name = normalize_lookup_name(team.get("name"))
    stripped_name = strip_team_stop_words(team.get("name"))
    normalized_label = normalize_lookup_name(label)
    stripped_label = strip_team_stop_words(label)
    normalized_aliases = [normalize_lookup_name(alias) for alias in aliases]
    stripped_aliases = [strip_team_stop_words(alias) for alias in aliases]

    score = 0
    if any(keyword in description for keyword in FOOTBALL_DESCRIPTION_KEYWORDS):
        score += 220
    if any(keyword in description for keyword in NON_TEAM_DESCRIPTION_KEYWORDS):
        score -= 180
    if normalized_label == normalized_name:
        score += 80
    if stripped_label and stripped_label == stripped_name:
        score += 140
    if normalized_name in normalized_aliases or stripped_name in stripped_aliases:
        score += 120
    if any(
        normalized_name and normalized_name in alias for alias in normalized_aliases
    ):
        score += 60
    if any(
        stripped_name and stripped_name in alias for alias in stripped_aliases
    ):
        score += 60
    if normalized_label and (
        normalized_label in normalized_name or normalized_name in normalized_label
    ):
        score += 25

    candidate_text = normalize_lookup_name(
        " ".join(
            [
                str(label or ""),
                str(candidate.get("description") or ""),
                *[str(alias) for alias in aliases],
            ]
        )
    )
    if normalized_name and all(
        token in candidate_text for token in normalized_name.split()
    ):
        score += 25

    country = str(team.get("country") or "").lower()
    if country and country in description:
        score += 20
    return score


def select_wikidata_candidate(
    team: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    minimum_score: int = 180,
) -> dict[str, Any] | None:
    ranked = sorted(
        candidates,
        key=lambda candidate: _candidate_score(team, candidate),
        reverse=True,
    )
    if not ranked:
        return None
    selected = ranked[0]
    if _candidate_score(team, selected) < minimum_score:
        return None
    return selected


def build_primary_team_translation_row(
    *,
    team: dict[str, Any],
    locale: str,
    display_name: str,
    source_name: str,
) -> dict[str, Any]:
    return {
        "id": f"{team['id']}:{locale}:primary",
        "team_id": str(team["id"]),
        "locale": locale,
        "display_name": display_name,
        "source_name": source_name,
        "is_primary": True,
    }


def build_primary_translation_rows_from_mapping(
    *,
    teams: list[dict[str, Any]],
    translation_map: dict[str, str],
    locale: str,
    source_name: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    misses: list[dict[str, Any]] = []
    for team in teams:
        team_id = str(team.get("id") or "")
        display_name = translation_map.get(team_id)
        if not display_name:
            misses.append(
                {
                    "id": team_id,
                    "name": str(team.get("name") or ""),
                    "reason": "mapped_name_not_found",
                }
            )
            continue
        rows.append(
            build_primary_team_translation_row(
                team=team,
                locale=locale,
                display_name=display_name,
                source_name=source_name,
            )
        )
    return rows, misses


def load_curated_translation_map(locale: str) -> dict[str, str]:
    target = DATA_DIR / f"team_translations_{locale}.json"
    if not target.exists():
        return {}
    payload = json.loads(target.read_text())
    if not isinstance(payload, dict):
        return {}
    return {
        str(team_id): str(display_name).strip()
        for team_id, display_name in payload.items()
        if str(display_name).strip()
    }


def build_wikidata_primary_translation_rows(
    teams: list[dict[str, Any]],
    *,
    locale: str,
    source_name: str,
    search_fn=None,
    labels_fn=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    search = search_fn or search_wikidata_entities
    fetch_labels = labels_fn or fetch_wikidata_labels
    candidate_by_team_id: dict[str, dict[str, Any]] = {}
    misses: list[dict[str, Any]] = []
    for team in teams:
        selected = select_wikidata_candidate(
            team,
            search(str(team.get("name") or "")),
        )
        if selected is None:
            misses.append(
                {
                    "id": str(team.get("id") or ""),
                    "name": str(team.get("name") or ""),
                    "reason": "candidate_not_found",
                }
            )
            continue
        candidate_by_team_id[str(team["id"])] = selected

    labels_by_entity_id = fetch_labels(
        [str(candidate["id"]) for candidate in candidate_by_team_id.values()],
        language=locale,
    )

    rows: list[dict[str, Any]] = []
    for team in teams:
        candidate = candidate_by_team_id.get(str(team.get("id") or ""))
        if candidate is None:
            continue
        display_name = labels_by_entity_id.get(str(candidate["id"]))
        if not display_name:
            misses.append(
                {
                    "id": str(team.get("id") or ""),
                    "name": str(team.get("name") or ""),
                    "reason": "localized_label_not_found",
                }
            )
            continue
        rows.append(
            build_primary_team_translation_row(
                team=team,
                locale=locale,
                display_name=display_name,
                source_name=source_name,
            )
        )

    return rows, misses


def filter_missing_primary_translations(
    teams: list[dict[str, Any]],
    existing_rows: list[dict[str, Any]],
    *,
    locale: str,
) -> list[dict[str, Any]]:
    existing_team_ids = {
        str(row.get("team_id"))
        for row in existing_rows
        if str(row.get("locale") or "").lower() == locale.lower()
        and bool(row.get("is_primary"))
    }
    return [
        team for team in teams if str(team.get("id")) not in existing_team_ids
    ]


def search_wikidata_entities(search_term: str) -> list[dict[str, Any]]:
    params = urlencode(
        {
            "action": "wbsearchentities",
            "search": search_term,
            "language": "en",
            "type": "item",
            "limit": "8",
            "format": "json",
        }
    )
    request = Request(
        url=f"{WIKIDATA_API_URL}?{params}",
        headers={
            "User-Agent": WIKIDATA_USER_AGENT,
            "Accept": "application/json",
        },
        method="GET",
    )
    return cached_request_json(
        cache_dir=WIKIDATA_CACHE_DIR,
        cache_namespace="wikidata-search",
        cache_key=search_term,
        request_or_url=request,
    ).get("search", [])


def fetch_wikidata_labels(
    entity_ids: list[str],
    *,
    language: str,
) -> dict[str, str]:
    if not entity_ids:
        return {}
    labels: dict[str, str] = {}
    for index in range(0, len(entity_ids), 50):
        params = urlencode(
            {
                "action": "wbgetentities",
                "ids": "|".join(entity_ids[index : index + 50]),
                "languages": f"{language}|en",
                "props": "labels|sitelinks",
                "format": "json",
            }
        )
        request = Request(
            url=f"{WIKIDATA_API_URL}?{params}",
            headers={
                "User-Agent": WIKIDATA_USER_AGENT,
                "Accept": "application/json",
            },
            method="GET",
        )
        payload = cached_request_json(
            cache_dir=WIKIDATA_CACHE_DIR,
            cache_namespace=f"wikidata-labels-{language}",
            cache_key="|".join(entity_ids[index : index + 50]),
            request_or_url=request,
        )

        for entity_id, entity in payload.get("entities", {}).items():
            localized = (((entity.get("labels") or {}).get(language) or {}).get("value"))
            if isinstance(localized, str) and localized.strip():
                labels[entity_id] = localized.strip()
                continue
            localized_title = (
                ((entity.get("sitelinks") or {}).get(f"{language}wiki") or {}).get("title")
            )
            if isinstance(localized_title, str) and localized_title.strip():
                labels[entity_id] = localized_title.strip().replace("_", " ")
    return labels


def request_json(
    request_or_url: Request | str,
    *,
    opener=urlopen,
    sleep_fn=time.sleep,
    throttle_seconds: float = WIKIDATA_THROTTLE_SECONDS,
    retries: int = 4,
) -> dict[str, Any]:
    for attempt in range(retries + 1):
        if throttle_seconds > 0:
            sleep_fn(throttle_seconds)
        try:
            request = (
                request_or_url
                if isinstance(request_or_url, Request)
                else Request(request_or_url)
            )
            with opener(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code != 429 or attempt == retries:
                raise
            sleep_fn(float(2**attempt))
    return {}


def cached_request_json(
    *,
    cache_dir: Path,
    cache_namespace: str,
    cache_key: str,
    request_or_url: Request | str,
    opener=urlopen,
    sleep_fn=time.sleep,
    throttle_seconds: float = WIKIDATA_THROTTLE_SECONDS,
    retries: int = 4,
) -> dict[str, Any]:
    namespace_dir = cache_dir / cache_namespace
    namespace_dir.mkdir(parents=True, exist_ok=True)
    cache_name = sha1(cache_key.encode("utf-8")).hexdigest()
    cache_path = namespace_dir / f"{cache_name}.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text())

    payload = request_json(
        request_or_url,
        opener=opener,
        sleep_fn=sleep_fn,
        throttle_seconds=throttle_seconds,
        retries=retries,
    )
    cache_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
    )
    return payload
