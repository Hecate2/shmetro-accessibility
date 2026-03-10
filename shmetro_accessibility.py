#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import html
import json
import re
import httpx
import aiosqlite
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from tqdm import tqdm

STATION_URL = "https://m.shmetro.com/core/shmetro/mdstationinfoback_new.ashx"
TRIP_URL = "https://m.shmetro.com/interface/plantrip/pt.aspx"
LINE_NUMBERS = [*range(1, 19), 41, 51]
OPTION_PATTERN = re.compile(r'<option\s+value="(?P<id>[^"]+)"[^>]*>(?P<name>.*?)</option>', re.IGNORECASE)
HEADERS = {
    # "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass(frozen=True)
class Station:
    station_id: str
    station_name: str
    line: int


class UnionFind:
    def __init__(self, nodes: Iterable[str]) -> None:
        self.parent: Dict[str, str] = {n: n for n in nodes}
        self.rank: Dict[str, int] = {n: 0 for n in nodes}

    def find(self, node: str) -> str:
        parent = self.parent[node]
        if parent != node:
            self.parent[node] = self.find(parent)
        return self.parent[node]

    def union(self, a: str, b: str) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


class MetroCrawler:
    def __init__(self, pause_sec: float = 0.15, timeout_sec: int = 15, retries: int = 3) -> None:
        self.pause_sec = pause_sec
        self.timeout_sec = timeout_sec
        self.retries = retries
        self.client = httpx.AsyncClient(headers=HEADERS, timeout=self.timeout_sec)

    async def aclose(self) -> None:
        await self.client.aclose()

    async def _fetch(self, url: str, params: Dict[str, str]) -> str:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = await self.client.get(url, params=params, timeout=self.timeout_sec)
                response.raise_for_status()
                body = response.text
                await asyncio.sleep(self.pause_sec)
                return body
            except (httpx.HTTPError, TimeoutError) as exc:
                last_exc = exc
                if attempt < self.retries:
                    await asyncio.sleep(0.8 * attempt)
                else:
                    break
            except (OSError, UnicodeDecodeError) as exc:
                last_exc = exc
                if attempt < self.retries:
                    await asyncio.sleep(0.8 * attempt)
                else:
                    break
        raise RuntimeError(f"Request failed after {self.retries} retries: {url}; params={params}; error={last_exc}")

    async def fetch_stations_by_line(self, line_number: int) -> List[Station]:
        html_body = await self._fetch(STATION_URL, {"act": "slsddl", "ln": str(line_number), "sc": ""})
        stations: List[Station] = []
        for match in OPTION_PATTERN.finditer(html_body):
            station_id = match.group("id").strip()
            station_name = html.unescape(match.group("name").strip())
            if station_id:
                stations.append(Station(station_id=station_id, station_name=station_name, line=line_number))
        return stations

    async def fetch_trip_impedance(self, start_id: str, end_id: str) -> Tuple[Optional[int], bool]:
        # print(f"Fetching trip impedance: {start_id} -> {end_id}")
        body = await self._fetch(
            TRIP_URL,
            {
                "func": "plantrip",
                "startId": start_id,
                "endId": end_id,
                "planTime": "00:59",
                "week": "2",
                "ticket": "oneCard",
                "type": "0",
            },
        )
        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON for {start_id}->{end_id}: {body[:300]}") from exc

        path_list = data.get("pathList") or []
        if not path_list:
            return 0, True

        first_path = path_list[0] if isinstance(path_list, list) else {}
        raw_value = first_path.get("impedancevalue")
        if raw_value is None or raw_value == "":
            return None, False
        try:
            return int(raw_value), False
        except ValueError:
            return None, False


async def build_station_catalog(crawler: MetroCrawler) -> Tuple[List[Station], Dict[int, List[Station]]]:
    per_line: Dict[int, List[Station]] = {}
    all_stations: List[Station] = []

    for line_number in LINE_NUMBERS:
        stations = await crawler.fetch_stations_by_line(line_number)
        per_line[line_number] = stations
        all_stations.extend(stations)
        print(f"Fetched line {line_number}: {len(stations)} stations")

    deduped: Dict[str, Station] = {}
    for station in all_stations:
        deduped[station.station_id] = station

    unique_stations = sorted(deduped.values(), key=lambda s: s.station_id)
    return unique_stations, per_line


def load_station_catalog_from_csv(csv_path: Path) -> Tuple[List[Station], Dict[int, List[Station]]]:
    per_line: Dict[int, List[Station]] = {line_no: [] for line_no in LINE_NUMBERS}
    all_stations: List[Station] = []

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                line_no = int(row["line"])
                station_id = row["station_id"]
                station_name = row["station_name"]
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError(f"Invalid station cache format in {csv_path}") from exc

            station = Station(station_id=station_id, station_name=station_name, line=line_no)
            per_line.setdefault(line_no, []).append(station)
            all_stations.append(station)

    deduped: Dict[str, Station] = {}
    for station in all_stations:
        deduped[station.station_id] = station
    unique_stations = sorted(deduped.values(), key=lambda s: s.station_id)
    return unique_stations, per_line


async def init_db(db_path: Path) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(str(db_path), timeout=30, isolation_level=None)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await conn.execute("PRAGMA temp_store=MEMORY;")
    await conn.execute("PRAGMA busy_timeout=5000;")
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS times (from_id TEXT NOT NULL, to_id TEXT NOT NULL, minutes INTEGER, PRIMARY KEY(from_id,to_id))"
    )
    return conn


async def flush_wal_logs(db_path: Path) -> None:
    if not db_path.exists():
        return
    conn = await aiosqlite.connect(str(db_path), timeout=30, isolation_level=None)
    try:
        await conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        print("WAL checkpoint(TRUNCATE) done before crawl")
    finally:
        await conn.close()


def canonical_pair(a: str, b: str) -> Tuple[str, str]:
    return (a, b) if a <= b else (b, a)


async def load_time_map_and_uf(db_path: Path, stations: List[Station]) -> Tuple[Dict[Tuple[str, str], Optional[int]], UnionFind]:
    ids = [station.station_id for station in stations]
    uf = UnionFind(ids)
    time_map: Dict[Tuple[str, str], Optional[int]] = {}
    for sid in ids:
        time_map[(sid, sid)] = 0

    if not db_path.exists():
        return time_map, uf

    conn = await aiosqlite.connect(str(db_path), timeout=30, isolation_level=None)
    cur = await conn.execute("SELECT from_id, to_id, minutes FROM times")
    rows = await cur.fetchall()
    await cur.close()
    await conn.close()

    for a, b, mins in rows:
        ca, cb = canonical_pair(a, b)
        time_map[(ca, cb)] = mins
        time_map[(cb, ca)] = mins
        if ca != cb and mins == 0:
            uf.union(ca, cb)
    return time_map, uf


async def compute_times(
    crawler: MetroCrawler,
    stations: List[Station],
    db_path: Path,
    workers: int = 16,
) -> Tuple[Dict[Tuple[str, str], Optional[int]], UnionFind]:
    ids = [station.station_id for station in stations]
    id_set = set(ids)
    uf = UnionFind(ids)

    # prepare time map
    time_map: Dict[Tuple[str, str], Optional[int]] = {}
    for station_id in ids:
        time_map[(station_id, station_id)] = 0

    conn = await init_db(db_path)

    cur = await conn.execute("SELECT from_id, to_id, minutes FROM times")
    db_rows = await cur.fetchall()
    await cur.close()
    loaded = 0
    for a, b, mins in db_rows:
        ca, cb = canonical_pair(a, b)
        if ca not in id_set or cb not in id_set:
            continue
        time_map[(ca, cb)] = mins
        time_map[(cb, ca)] = mins
        if ca != cb and mins == 0:
            uf.union(ca, cb)
        loaded += 1
    if loaded:
        print(f"Recovered rows from DB: {loaded}")

    # Retry previously failed rows first (minutes IS NULL), likely from transient server errors.
    cur = await conn.execute("SELECT from_id, to_id FROM times WHERE minutes IS NULL")
    null_rows = await cur.fetchall()
    await cur.close()
    null_pairs: List[Tuple[str, str]] = []
    for a, b in null_rows:
        ca, cb = canonical_pair(a, b)
        if ca == cb:
            continue
        if ca not in id_set or cb not in id_set:
            continue
        null_pairs.append((ca, cb))
    if null_pairs:
        print(f"Found {len(null_pairs)} NULL rows; retrying these first")

    total_pairs = len(ids) * (len(ids) - 1) // 2
    completed_pairs = sum(
        1
        for i in range(len(ids))
        for j in range(i + 1, len(ids))
        if time_map.get((ids[i], ids[j])) is not None
    )
    remaining_pairs = total_pairs - completed_pairs
    print(f"Resuming compute_times: {completed_pairs} done, {remaining_pairs} remaining (of {total_pairs})")

    # build list of normal missing pairs
    missing_pairs: List[Tuple[str, str]] = []
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            a, b = canonical_pair(ids[i], ids[j])
            if (a, b) not in time_map:
                missing_pairs.append((a, b))

    # query order: retry NULL rows first, then missing rows
    pairs: List[Tuple[str, str]] = []
    seen_pairs: set[Tuple[str, str]] = set()
    for pair in null_pairs + missing_pairs:
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        pairs.append(pair)

    async def fetch_pair(pair: Tuple[str, str]) -> Tuple[str, str, Optional[int], bool]:
        a, b = pair
        mins, same = await crawler.fetch_trip_impedance(a, b)
        return a, b, mins, same

    # run with async task pool and write to sqlite after each fetch
    with tqdm(total=total_pairs, initial=completed_pairs, desc="Pair crawl", unit="pair") as pbar:
        pending: Dict[asyncio.Task[Tuple[str, str, Optional[int], bool]], Tuple[str, str]] = {}
        pair_iter = iter(pairs)

        def fill_pending() -> None:
            while len(pending) < workers:
                try:
                    pair = next(pair_iter)
                except StopIteration:
                    break
                task = asyncio.create_task(fetch_pair(pair))
                pending[task] = pair

        fill_pending()

        while pending:
            done, _ = await asyncio.wait(pending.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                a, b = pending.pop(task)
                try:
                    a, b, mins, same = task.result()
                except Exception:
                    try:
                        mins, same = await crawler.fetch_trip_impedance(a, b)
                    except Exception:
                        mins, same = None, False
                if same:
                    uf.union(a, b)
                    mins = 0

                # update map in both directions for downstream consumers
                time_map[(a, b)] = mins
                time_map[(b, a)] = mins

                # write a single canonical row into database immediately
                await conn.execute(
                    "INSERT OR REPLACE INTO times(from_id,to_id,minutes) VALUES(?,?,?)",
                    (a, b, mins),
                )

                if mins is not None:
                    completed_pairs += 1
                    pbar.update(1)

            fill_pending()

    await conn.close()
    return time_map, uf


def save_checkpoint(time_map: Dict[Tuple[str, str], Optional[int]], checkpoint_path: Path) -> None:
    rows = [{"from": key[0], "to": key[1], "minutes": value} for key, value in time_map.items()]
    checkpoint_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def write_stations_human_readable(per_line: Dict[int, List[Station]], output_dir: Path) -> None:
    md_path = output_dir / "stations_by_line.md"
    csv_path = output_dir / "stations_all.csv"

    lines: List[str] = ["# Shanghai Metro Stations", ""]
    for line_no in LINE_NUMBERS:
        lines.append(f"## Line {line_no}")
        stations = per_line.get(line_no, [])
        if not stations:
            lines.append("- (no station returned)")
        else:
            for station in stations:
                lines.append(f"- {station.station_name} ({station.station_id})")
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["line", "station_id", "station_name"])
        for line_no in LINE_NUMBERS:
            for station in per_line.get(line_no, []):
                writer.writerow([line_no, station.station_id, station.station_name])


def write_time_matrix(stations: List[Station], time_map: Dict[Tuple[str, str], Optional[int]], output_dir: Path) -> None:
    matrix_csv = output_dir / "travel_time_matrix.csv"
    readable_md = output_dir / "travel_time_pairs.md"

    station_ids = [station.station_id for station in stations]
    id_to_name = {station.station_id: station.station_name for station in stations}

    with matrix_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["from_id", "from_name", "to_id", "to_name", "minutes"])
        for sid_a in station_ids:
            for sid_b in station_ids:
                writer.writerow([sid_a, id_to_name[sid_a], sid_b, id_to_name[sid_b], time_map.get((sid_a, sid_b))])

    md_lines = ["# Shanghai Metro Pairwise Time Cost", "", "| From | To | Minutes |", "|---|---|---:|"]
    for i, sid_a in enumerate(station_ids):
        for sid_b in station_ids[i + 1 :]:
            minutes = time_map.get((sid_a, sid_b))
            md_lines.append(f"| {id_to_name[sid_a]} ({sid_a}) | {id_to_name[sid_b]} ({sid_b}) | {minutes} |")
    readable_md.write_text("\n".join(md_lines), encoding="utf-8")


def write_average_ranking(
    stations: List[Station],
    time_map: Dict[Tuple[str, str], Optional[int]],
    uf: UnionFind,
    output_dir: Path,
) -> None:
    ranking_csv = output_dir / "average_time_ranking.csv"
    ranking_md = output_dir / "average_time_ranking.md"

    station_ids = [station.station_id for station in stations]
    id_to_station = {station.station_id: station for station in stations}

    ranking: List[Tuple[str, str, float, int]] = []

    for sid_a in station_ids:
        values: List[int] = []
        for sid_b in station_ids:
            if sid_a == sid_b:
                continue
            if uf.find(sid_a) == uf.find(sid_b):
                continue
            minutes = time_map.get((sid_a, sid_b))
            # only include positive integer travel times; skip None and zeros
            if isinstance(minutes, int) and minutes > 0:
                values.append(minutes)

        average = sum(values) / len(values) if values else float("nan")
        ranking.append((sid_a, id_to_station[sid_a].station_name, average, len(values)))

    ranking.sort(key=lambda item: (float("inf") if item[2] != item[2] else item[2], item[0]))

    with ranking_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "station_id", "station_name", "average_minutes", "sample_size"])
        for idx, (sid, name, avg, sample_size) in enumerate(ranking, start=1):
            writer.writerow([idx, sid, name, f"{avg:.4f}" if avg == avg else "NaN", sample_size])

    lines = ["# Average Travel Time Ranking", "", "| Rank | Station | Average Minutes | Sample Size |", "|---:|---|---:|---:|"]
    for idx, (sid, name, avg, sample_size) in enumerate(ranking, start=1):
        avg_text = f"{avg:.4f}" if avg == avg else "NaN"
        lines.append(f"| {idx} | {name} ({sid}) | {avg_text} | {sample_size} |")
    ranking_md.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Shanghai metro travel-time accessibility crawler")
    parser.add_argument("--output", default="output", help="Output directory")
    parser.add_argument("--pause", type=float, default=0.15, help="Pause seconds between HTTP calls")
    parser.add_argument("--timeout", type=int, default=5, help="HTTP timeout seconds")
    parser.add_argument("--retries", type=int, default=1, help="Retry count for failed HTTP calls")
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Number of worker threads for fetching pair times",
    )
    parser.add_argument(
        "--compute-only",
        action="store_true",
        help="Skip network crawling; compute outputs only using existing station files and database",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "time_map.db"
    await flush_wal_logs(db_path)

    crawler = MetroCrawler(pause_sec=args.pause, timeout_sec=args.timeout, retries=args.retries)
    try:
        stations_csv_path = output_dir / "stations_all.csv"
        stations_md_path = output_dir / "stations_by_line.md"

        if args.compute_only:
            if not (stations_csv_path.exists() and stations_md_path.exists() and db_path.exists()):
                raise RuntimeError("compute-only requested but cache files missing")
            print("compute-only: loading from cache")
            stations, per_line = load_station_catalog_from_csv(stations_csv_path)
            time_map, uf = await load_time_map_and_uf(db_path, stations)
        else:
            if stations_csv_path.exists() and stations_md_path.exists():
                print("Station files found; skip station crawling and load from cache")
                stations, per_line = load_station_catalog_from_csv(stations_csv_path)
            else:
                stations, per_line = await build_station_catalog(crawler)
                write_stations_human_readable(per_line, output_dir)

            time_map, uf = await compute_times(crawler, stations, db_path, workers=args.workers)

        write_time_matrix(stations, time_map, output_dir)
        write_average_ranking(stations, time_map, uf, output_dir)
    finally:
        await crawler.aclose()

    print(f"Done. Output files saved in: {output_dir.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
