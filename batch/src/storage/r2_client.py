class R2Client:
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    def archive_json(self, key: str, payload: dict) -> str:
        return f"r2://{self.bucket}/{key}"
