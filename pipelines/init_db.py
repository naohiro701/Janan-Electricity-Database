from __future__ import annotations

from pathlib import Path
from time import sleep

import psycopg

from config import DATABASE_URL, ROOT_DIR


SCHEMA_PATH = ROOT_DIR / "schema.sql"


def main() -> None:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    sql = Path(SCHEMA_PATH).read_text(encoding="utf-8")
    last_error: Exception | None = None
    for attempt in range(1, 11):
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.transaction():
                    conn.execute(sql)
            return
        except Exception as exc:
            last_error = exc
            sleep(min(attempt, 5))
    raise RuntimeError(f"failed to initialize schema after retries: {last_error}") from last_error


if __name__ == "__main__":
    main()
