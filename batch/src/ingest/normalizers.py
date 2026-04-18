def normalize_team_name(name: str, aliases: dict[str, str]) -> str:
    return aliases.get(name, name)
