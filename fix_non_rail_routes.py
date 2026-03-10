#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

ALLOWED_PREFIXES = ("地铁", "市域", "轨道交通浦江线")


def build_where_clause() -> str:
    allowed = " OR ".join("summary LIKE ?" for _ in ALLOWED_PREFIXES)
    return f"NOT ({allowed})"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mark route_times rows as error when summary does not start with rail prefixes"
    )
    parser.add_argument(
        "--db-path",
        default="output/amap_transit.db",
        help="Path to the SQLite database",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the number of affected rows without updating the database",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    patterns = tuple(f"{prefix}%" for prefix in ALLOWED_PREFIXES)
    where_clause = build_where_clause()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            f"SELECT COUNT(*) FROM route_times WHERE {where_clause}",
            patterns,
        )
        affected_rows = cursor.fetchone()[0]

        if args.dry_run:
            print(f"Rows that would be updated: {affected_rows}")
            return

        conn.execute(
            f"UPDATE route_times SET status = 'error' WHERE {where_clause}",
            patterns,
        )
        conn.commit()
        print(f"Rows updated to error: {affected_rows}")


if __name__ == "__main__":
    main()