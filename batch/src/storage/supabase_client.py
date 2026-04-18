class SupabaseClient:
    def __init__(self, base_url: str, service_key: str) -> None:
        self.base_url = base_url
        self.service_key = service_key

    def upsert_rows(self, table: str, rows: list[dict]) -> int:
        return len(rows)
