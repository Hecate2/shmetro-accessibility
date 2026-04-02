#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import math
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import aiosqlite
from tqdm import tqdm

from amap_accessibility_common import (
    AMAP_POI_URL,
    AMAP_TRANSIT_URL,
    AMapClient,
    AMapCredentialConfig,
    RouteResult,
    route_result_is_final,
    select_transit,
)

METRO_POI_TYPECODE = "150500"
POI_TYPE_STATION = "交通设施服务;地铁站;地铁站"
TRAILING_PARENTHETICAL_RE = re.compile(r"\s*[（(][^()（）]+[)）]\s*$")


@dataclass(frozen=True)
class Station:
    station_id: str
    station_slug: str
    station_name: str
    line_order: int
    line_label: str
    source_key: str


@dataclass(frozen=True)
class ResolvedStation:
    station_id: str
    station_slug: str
    station_name: str
    line_order: int
    line_label: str
    source_key: str
    query_text: str
    poi_id: str
    poi_name: str
    poi_type: str
    poi_address: str
    location: str
    status: str
    score: int
    note: str


@dataclass(frozen=True)
class StationCatalogLoadResult:
    stations: List[Station]
    source: str


@dataclass(frozen=True)
class StationResolveRules:
    choose_queries: Callable[[Station], List[str]]
    choose_regions: Callable[[Station], List[str]]
    choose_poi_types: Callable[[Station], List[Optional[str]]]
    candidate_score: Callable[[Station, Dict[str, Any]], Tuple[int, str]]
    route_city_code: Callable[[Station], str]
    accepted_score: int = 110


STATION_AMAP_COLUMNS = (
    "station_id",
    "station_slug",
    "station_name",
    "line_order",
    "line_label",
    "source_key",
    "query_text",
    "poi_id",
    "poi_name",
    "poi_type",
    "poi_address",
    "location",
    "status",
    "score",
    "note",
)
LEGACY_SHANGHAI_STATION_AMAP_COLUMNS = (
    "station_id",
    "line",
    "station_name",
    "line_label",
    "query_text",
    "poi_id",
    "poi_name",
    "poi_type",
    "poi_address",
    "location",
    "status",
    "score",
    "note",
)
STATION_AMAP_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS station_amap (
        station_id TEXT PRIMARY KEY,
        station_slug TEXT NOT NULL,
        station_name TEXT NOT NULL,
        line_order INTEGER NOT NULL,
        line_label TEXT NOT NULL,
        source_key TEXT NOT NULL,
        query_text TEXT NOT NULL,
        poi_id TEXT,
        poi_name TEXT,
        poi_type TEXT,
        poi_address TEXT,
        location TEXT,
        status TEXT NOT NULL,
        score INTEGER NOT NULL DEFAULT 0,
        note TEXT NOT NULL DEFAULT ''
    )
"""
ROUTE_TIMES_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS route_times (
        from_id TEXT NOT NULL,
        to_id TEXT NOT NULL,
        status TEXT NOT NULL,
        duration_seconds INTEGER,
        transit_index INTEGER,
        summary TEXT NOT NULL DEFAULT '',
        reason TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(from_id, to_id)
    )
"""


def build_standard_parser(
    *,
    description: str,
    default_output: str,
    default_stations_html: str,
    default_db_path: str,
    default_service_date_value: str,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--output", default=default_output, help="Output directory")
    parser.add_argument(
        "--stations-html",
        default=default_stations_html,
        help="MetroMan station-list HTML file",
    )
    parser.add_argument("--db-path", default=default_db_path, help="SQLite database path")
    parser.add_argument("--env-file", default=".env", help="Environment file containing KEY and SEC")
    parser.add_argument("--pause", type=float, default=0.0, help="Optional extra delay after successful AMap requests")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--retries", type=int, default=4, help="Retry count for transient AMap errors")
    parser.add_argument("--resolve-workers", type=int, default=2, help="Concurrent workers for station matching")
    parser.add_argument("--route-workers", type=int, default=6, help="Concurrent workers for route crawling")
    parser.add_argument(
        "--max-routes",
        type=int,
        default=0,
        help="Optional cap for how many unresolved routes to crawl in this run; 0 means no cap",
    )
    parser.add_argument("--station-search-qps", type=float, default=3.01, help="Hard QPS cap for AMap station search requests")
    parser.add_argument("--route-plan-qps", type=float, default=3.01, help="Hard QPS cap for AMap route planning requests")
    parser.add_argument("--date", default=default_service_date_value, help="Service date in YYYY-MM-DD, defaults to a workday")
    parser.add_argument("--time", default="7:15", help="Departure time, for example 7:15")
    parser.add_argument("--strategy", default="0", help="AMap transit strategy, default 0 is the auto-recommended route")
    parser.add_argument("--resolve-only", action="store_true", help="Only resolve station nodes without crawling routes")
    parser.add_argument("--compute-only", action="store_true", help="Skip network calls and only rebuild outputs from sqlite")
    return parser


def collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def dedupe_strings(values: Sequence[str]) -> List[str]:
    deduped: List[str] = []
    for value in values:
        candidate = collapse_whitespace(value)
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def build_station_id(line_order: int, station_slug: str) -> str:
    return f"{line_order:02d}-{station_slug}"


def strip_trailing_parenthetical(value: str) -> str:
    candidate = collapse_whitespace(value)
    while True:
        updated = TRAILING_PARENTHETICAL_RE.sub("", candidate).strip()
        if not updated or updated == candidate:
            return candidate
        candidate = updated


def station_name_variants(station_name: str) -> List[str]:
    variants = [collapse_whitespace(station_name)]
    stripped_name = strip_trailing_parenthetical(station_name)
    if stripped_name and stripped_name not in variants:
        variants.append(stripped_name)
    return variants


class MetroManStationHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_section = False
        self.in_heading = False
        self.current_heading_parts: List[str] = []
        self.current_line_label = ""
        self.current_line_order = 0
        self._seen_station_ids: set[str] = set()
        self.stations: List[Station] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attrs_map = {key: value or "" for key, value in attrs}
        class_names = set(collapse_whitespace(attrs_map.get("class", "")).split())

        if tag == "section" and "line-group" in class_names:
            self.in_section = True
            self.current_line_order += 1
            self.current_line_label = ""
            return

        if self.in_section and tag == "h2":
            self.in_heading = True
            self.current_heading_parts = []
            return

        if self.in_section and tag == "li" and "station-item" in class_names:
            data_key = attrs_map.get("data-key", "").strip()
            if data_key and self.current_line_label:
                self._append_station(data_key)

    def handle_endtag(self, tag: str) -> None:
        if tag == "h2" and self.in_heading:
            self.current_line_label = collapse_whitespace("".join(self.current_heading_parts))
            self.in_heading = False
            self.current_heading_parts = []
            return

        if tag == "section" and self.in_section:
            self.in_section = False
            self.in_heading = False
            self.current_heading_parts = []

    def handle_data(self, data: str) -> None:
        if self.in_heading:
            self.current_heading_parts.append(data)

    def _append_station(self, data_key: str) -> None:
        parts = [part.strip() for part in data_key.split("|")]
        if len(parts) < 3:
            raise RuntimeError(f"Unexpected data-key format: {data_key}")

        station_slug = parts[0]
        station_name = parts[2]
        if not station_slug or not station_name:
            raise RuntimeError(f"Missing station slug or simplified Chinese name in data-key: {data_key}")

        station_id = build_station_id(self.current_line_order, station_slug)
        if station_id in self._seen_station_ids:
            return

        self._seen_station_ids.add(station_id)
        self.stations.append(
            Station(
                station_id=station_id,
                station_slug=station_slug,
                station_name=station_name,
                line_order=self.current_line_order,
                line_label=self.current_line_label,
                source_key=data_key,
            )
        )


def load_station_catalog_from_html(html_path: Path) -> List[Station]:
    parser = MetroManStationHTMLParser()
    parser.feed(html_path.read_text(encoding="utf-8"))
    parser.close()
    if not parser.stations:
        raise RuntimeError(f"No stations parsed from HTML: {html_path}")
    return parser.stations


async def _table_exists(conn: aiosqlite.Connection, table_name: str) -> bool:
    cursor = await conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    row = await cursor.fetchone()
    await cursor.close()
    return row is not None


async def _table_columns(conn: aiosqlite.Connection, table_name: str) -> List[str]:
    cursor = await conn.execute(f"PRAGMA table_info({table_name})")
    rows = await cursor.fetchall()
    await cursor.close()
    return [str(row[1]) for row in rows]


async def _create_station_amap_table(conn: aiosqlite.Connection) -> None:
    await conn.execute(STATION_AMAP_TABLE_SQL)


async def _migrate_legacy_station_amap(conn: aiosqlite.Connection) -> None:
    if not await _table_exists(conn, "station_amap"):
        return

    columns = await _table_columns(conn, "station_amap")
    if tuple(columns) == STATION_AMAP_COLUMNS:
        return

    if tuple(columns) != LEGACY_SHANGHAI_STATION_AMAP_COLUMNS:
        raise RuntimeError(f"Unsupported station_amap schema: {columns}")

    await conn.execute("ALTER TABLE station_amap RENAME TO station_amap_legacy")
    await _create_station_amap_table(conn)
    await conn.execute(
        """
        INSERT INTO station_amap(
            station_id, station_slug, station_name, line_order, line_label, source_key,
            query_text, poi_id, poi_name, poi_type, poi_address, location,
            status, score, note
        )
        SELECT
            station_id,
            station_id,
            station_name,
            line,
            line_label,
            station_id,
            query_text,
            poi_id,
            poi_name,
            poi_type,
            poi_address,
            location,
            status,
            score,
            note
        FROM station_amap_legacy
        """
    )
    await conn.execute("DROP TABLE station_amap_legacy")


async def _migrate_station_catalog_into_station_amap(conn: aiosqlite.Connection) -> None:
    if not await _table_exists(conn, "station_catalog"):
        return

    await _create_station_amap_table(conn)
    stations = await _load_station_catalog_rows(conn, "station_catalog")
    await sync_station_catalog(conn, stations)
    await conn.execute("DROP TABLE station_catalog")


async def init_db(db_path: Path) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(str(db_path), timeout=30, isolation_level=None)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await conn.execute("PRAGMA temp_store=MEMORY;")
    await conn.execute("PRAGMA busy_timeout=5000;")
    await _migrate_legacy_station_amap(conn)
    await _create_station_amap_table(conn)
    await conn.execute(ROUTE_TIMES_TABLE_SQL)
    await _migrate_station_catalog_into_station_amap(conn)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_station_amap_line_order ON station_amap(line_order, station_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_station_amap_status ON station_amap(status)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_route_times_status ON route_times(status)")
    return conn


async def sync_station_catalog(conn: aiosqlite.Connection, stations: Sequence[Station]) -> None:
    rows = [
        (
            station.station_id,
            station.station_slug,
            station.station_name,
            station.line_order,
            station.line_label,
            station.source_key,
        )
        for station in stations
    ]
    await conn.executemany(
        """
        INSERT OR IGNORE INTO station_amap(
            station_id, station_slug, station_name, line_order, line_label, source_key,
            query_text, poi_id, poi_name, poi_type, poi_address, location,
            status, score, note
        ) VALUES(?,?,?,?,?,?, '', '', '', '', '', '', 'unresolved', 0, 'catalog')
        """,
        rows,
    )
    await conn.executemany(
        """
        UPDATE station_amap
        SET station_slug=?, station_name=?, line_order=?, line_label=?, source_key=?
        WHERE station_id=?
        """,
        [
            (
                station.station_slug,
                station.station_name,
                station.line_order,
                station.line_label,
                station.source_key,
                station.station_id,
            )
            for station in stations
        ],
    )

    if not rows:
        await conn.execute("DELETE FROM route_times")
        await conn.execute("DELETE FROM station_amap")
        return

    placeholders = ", ".join("?" for _ in rows)
    await conn.execute(
        f"DELETE FROM route_times WHERE from_id NOT IN ({placeholders}) OR to_id NOT IN ({placeholders})",
        [station.station_id for station in stations] + [station.station_id for station in stations],
    )
    await conn.execute(
        f"DELETE FROM station_amap WHERE station_id NOT IN ({placeholders})",
        [station.station_id for station in stations],
    )


async def _load_station_catalog_rows(conn: aiosqlite.Connection, table_name: str) -> List[Station]:
    cursor = await conn.execute(
        f"SELECT station_id, station_slug, station_name, line_order, line_label, source_key FROM {table_name} ORDER BY line_order, station_id"
    )
    rows = await cursor.fetchall()
    await cursor.close()
    return [Station(*row) for row in rows]


async def load_station_catalog_with_source(conn: aiosqlite.Connection) -> StationCatalogLoadResult:
    stations = await _load_station_catalog_rows(conn, "station_amap")
    if stations:
        return StationCatalogLoadResult(stations=stations, source="station_amap")

    raise RuntimeError("Station source file not found and station_amap is empty. Provide a station source file or populate the database first.")


async def load_or_sync_station_catalog(conn: aiosqlite.Connection, html_path: Path) -> StationCatalogLoadResult:
    if html_path.exists():
        stations = load_station_catalog_from_html(html_path)
        await sync_station_catalog(conn, stations)
        return StationCatalogLoadResult(stations=stations, source="html")

    return await load_station_catalog_with_source(conn)


async def load_station_catalog_from_db(conn: aiosqlite.Connection) -> List[Station]:
    return (await load_station_catalog_with_source(conn)).stations


async def load_resolved_stations(conn: aiosqlite.Connection) -> Dict[str, ResolvedStation]:
    cursor = await conn.execute(
        "SELECT station_id, station_slug, station_name, line_order, line_label, source_key, query_text, poi_id, poi_name, poi_type, poi_address, location, status, score, note FROM station_amap"
    )
    rows = await cursor.fetchall()
    await cursor.close()
    resolved: Dict[str, ResolvedStation] = {}
    for row in rows:
        record = ResolvedStation(*row)
        resolved[record.station_id] = record
    return resolved


async def load_route_results(conn: aiosqlite.Connection) -> Dict[Tuple[str, str], RouteResult]:
    cursor = await conn.execute(
        "SELECT from_id, to_id, status, duration_seconds, transit_index, summary, reason FROM route_times"
    )
    rows = await cursor.fetchall()
    await cursor.close()
    results: Dict[Tuple[str, str], RouteResult] = {}
    for row in rows:
        result = RouteResult(*row)
        results[(result.from_id, result.to_id)] = result
    return results


def resolved_station_can_plan_route(record: Optional[ResolvedStation]) -> bool:
    return record is not None and record.status == "resolved" and bool(record.location) and bool(record.poi_id)


class MetroAMapClient(AMapClient):
    def __init__(
        self,
        credentials: Sequence[AMapCredentialConfig],
        pause_sec: float = 0.0,
        timeout_sec: int = 20,
        retries: int = 4,
        station_search_qps: float = 3.01,
        route_plan_qps: float = 3.01,
        search_page_size: int = 10,
    ) -> None:
        super().__init__(
            credentials=credentials,
            pause_sec=pause_sec,
            timeout_sec=timeout_sec,
            retries=retries,
            station_search_qps=station_search_qps,
            route_plan_qps=route_plan_qps,
        )
        if not 1 <= search_page_size <= 25:
            raise ValueError("search_page_size must be between 1 and 25")
        self.search_page_size = search_page_size

    async def search_station_candidates_with_types(
        self,
        query_text: str,
        region: str,
        poi_types: Optional[str],
    ) -> List[Dict[str, Any]]:
        credential = await self._acquire_station_search_credential()
        params: Dict[str, Any] = {
            "keywords": query_text,
            "region": region,
            "city_limit": "true",
            "show_fields": "business",
            "page_size": str(self.search_page_size),
            "page_num": "1",
            "output": "JSON",
        }
        if poi_types:
            params["types"] = poi_types

        payload = await self._request_json(AMAP_POI_URL, params, credential)
        return list(payload.get("pois") or [])

    async def route_transit(
        self,
        origin: str,
        destination: str,
        origin_poi: str,
        destination_poi: str,
        city1: str,
        city2: str,
        service_date: str,
        service_time: str,
        strategy: str,
    ) -> Dict[str, Any]:
        credential = await self._acquire_route_plan_credential()
        return await self._request_json(
            AMAP_TRANSIT_URL,
            {
                "origin": origin,
                "destination": destination,
                "originpoi": origin_poi,
                "destinationpoi": destination_poi,
                "city1": city1,
                "city2": city2,
                "strategy": strategy,
                "AlternativeRoute": "8",
                "nightflag": "0",
                "max_trans": "5",
                "date": service_date,
                "time": service_time,
                "show_fields": "cost",
                "output": "JSON",
            },
            credential,
        )


def _default_query_text(station: Station, rules: StationResolveRules) -> str:
    queries = rules.choose_queries(station)
    return queries[0] if queries else station.station_name


def _build_unresolved_station_record(station: Station, rules: StationResolveRules, reason: str) -> ResolvedStation:
    return ResolvedStation(
        station_id=station.station_id,
        station_slug=station.station_slug,
        station_name=station.station_name,
        line_order=station.line_order,
        line_label=station.line_label,
        source_key=station.source_key,
        query_text=_default_query_text(station, rules),
        poi_id="",
        poi_name="",
        poi_type="",
        poi_address="",
        location="",
        status="unresolved",
        score=0,
        note=reason,
    )


async def resolve_station_node(
    client: MetroAMapClient,
    station: Station,
    rules: StationResolveRules,
) -> ResolvedStation:
    best_record: Optional[ResolvedStation] = None
    queries = rules.choose_queries(station)
    regions = rules.choose_regions(station)
    poi_types_to_try = rules.choose_poi_types(station)

    for poi_types in poi_types_to_try:
        for region in regions:
            for query_text in queries:
                candidates = await client.search_station_candidates_with_types(query_text, region, poi_types)
                for poi in candidates:
                    score, note = rules.candidate_score(station, poi)
                    if score < 0:
                        continue

                    location = str(poi.get("location") or "")
                    poi_id = str(poi.get("id") or "")
                    if not location or not poi_id:
                        continue

                    record = ResolvedStation(
                        station_id=station.station_id,
                        station_slug=station.station_slug,
                        station_name=station.station_name,
                        line_order=station.line_order,
                        line_label=station.line_label,
                        source_key=station.source_key,
                        query_text=f"{query_text} [region={region};types={poi_types or 'ANY'}]",
                        poi_id=poi_id,
                        poi_name=str(poi.get("name") or ""),
                        poi_type=str(poi.get("type") or ""),
                        poi_address=str(poi.get("address") or ""),
                        location=location,
                        status="resolved",
                        score=score,
                        note=note,
                    )
                    if best_record is None or record.score > best_record.score:
                        best_record = record

                if best_record is not None and best_record.score >= rules.accepted_score:
                    return best_record

    if best_record is not None:
        return best_record

    raise RuntimeError(f"No station POI found for {station.line_label} {station.station_name}")


async def resolve_stations(
    client: MetroAMapClient,
    conn: aiosqlite.Connection,
    stations: Sequence[Station],
    workers: int,
    rules: StationResolveRules,
) -> Dict[str, ResolvedStation]:
    existing = await load_resolved_stations(conn)
    pending_stations = [
        station
        for station in stations
        if station.station_id not in existing or existing[station.station_id].status == "unresolved"
    ]
    if not pending_stations:
        return existing

    async def fetch_one(station: Station) -> ResolvedStation:
        return await resolve_station_node(client, station, rules)

    with tqdm(total=len(stations), initial=len(stations) - len(pending_stations), desc="Resolve stations", unit="station") as pbar:
        pending: Dict[asyncio.Task[ResolvedStation], Station] = {}
        station_iter = iter(pending_stations)

        def fill_pending() -> None:
            while len(pending) < workers:
                try:
                    station = next(station_iter)
                except StopIteration:
                    break
                task = asyncio.create_task(fetch_one(station))
                pending[task] = station

        fill_pending()
        while pending:
            done, _ = await asyncio.wait(pending.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                station = pending.pop(task)
                try:
                    result = task.result()
                except Exception as exc:
                    result = _build_unresolved_station_record(station, rules, f"error:{exc}")

                await conn.execute(
                    """
                    INSERT OR REPLACE INTO station_amap(
                        station_id, station_slug, station_name, line_order, line_label, source_key,
                        query_text, poi_id, poi_name, poi_type, poi_address, location,
                        status, score, note
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        result.station_id,
                        result.station_slug,
                        result.station_name,
                        result.line_order,
                        result.line_label,
                        result.source_key,
                        result.query_text,
                        result.poi_id,
                        result.poi_name,
                        result.poi_type,
                        result.poi_address,
                        result.location,
                        result.status,
                        result.score,
                        result.note,
                    ),
                )
                existing[result.station_id] = result
                pbar.update(1)
            fill_pending()

    return existing


async def crawl_routes(
    client: MetroAMapClient,
    conn: aiosqlite.Connection,
    stations: Sequence[Station],
    resolved_stations: Dict[str, ResolvedStation],
    workers: int,
    service_date: str,
    service_time: str,
    strategy: str,
    route_city_code: Callable[[Station], str],
    max_routes: Optional[int] = None,
) -> Dict[Tuple[str, str], RouteResult]:
    existing = await load_route_results(conn)
    resolved_ids = {
        station_id
        for station_id, record in resolved_stations.items()
        if resolved_station_can_plan_route(record)
    }

    pairs: List[Tuple[Station, Station]] = []
    for origin in stations:
        for destination in stations:
            if origin.station_id == destination.station_id:
                continue
            if origin.station_id not in resolved_ids or destination.station_id not in resolved_ids:
                continue
            current = existing.get((origin.station_id, destination.station_id))
            if route_result_is_final(current):
                continue
            pairs.append((origin, destination))

    if max_routes is not None and max_routes > 0:
        pairs = pairs[:max_routes]

    completed = 0
    for origin in stations:
        for destination in stations:
            if origin.station_id == destination.station_id:
                continue
            if origin.station_id not in resolved_ids or destination.station_id not in resolved_ids:
                continue
            current = existing.get((origin.station_id, destination.station_id))
            if route_result_is_final(current):
                completed += 1

    total = completed + len(pairs)
    if not pairs:
        return existing

    async def fetch_one(pair: Tuple[Station, Station]) -> RouteResult:
        origin, destination = pair
        origin_resolved = resolved_stations[origin.station_id]
        destination_resolved = resolved_stations[destination.station_id]
        payload = await client.route_transit(
            origin=origin_resolved.location,
            destination=destination_resolved.location,
            origin_poi=origin_resolved.poi_id,
            destination_poi=destination_resolved.poi_id,
            city1=route_city_code(origin),
            city2=route_city_code(destination),
            service_date=service_date,
            service_time=service_time,
            strategy=strategy,
        )
        return select_transit(payload, origin.station_id, destination.station_id)

    with tqdm(total=total, initial=completed, desc="Crawl routes", unit="route") as pbar:
        pending: Dict[asyncio.Task[RouteResult], Tuple[Station, Station]] = {}
        pair_iter = iter(pairs)

        def fill_pending() -> None:
            while len(pending) < workers:
                try:
                    pair = next(pair_iter)
                except StopIteration:
                    break
                task = asyncio.create_task(fetch_one(pair))
                pending[task] = pair

        fill_pending()
        while pending:
            done, _ = await asyncio.wait(pending.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                origin, destination = pending.pop(task)
                try:
                    result = task.result()
                except Exception as exc:
                    result = RouteResult(
                        from_id=origin.station_id,
                        to_id=destination.station_id,
                        status="error",
                        duration_seconds=None,
                        transit_index=None,
                        summary="",
                        reason=str(exc),
                    )

                await conn.execute(
                    "INSERT OR REPLACE INTO route_times(from_id, to_id, status, duration_seconds, transit_index, summary, reason) VALUES(?,?,?,?,?,?,?)",
                    (
                        result.from_id,
                        result.to_id,
                        result.status,
                        result.duration_seconds,
                        result.transit_index,
                        result.summary,
                        result.reason,
                    ),
                )
                existing[(result.from_id, result.to_id)] = result
                if result.status in {"done", "no_valid_route"}:
                    pbar.update(1)
            fill_pending()

    return existing


def write_station_catalog(stations: Sequence[Station], output_dir: Path, network_name: str) -> None:
    csv_path = output_dir / "stations_all.csv"
    md_path = output_dir / "stations_by_line.md"

    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["line_order", "line_label", "station_id", "station_slug", "station_name", "source_key"])
        for station in stations:
            writer.writerow(
                [
                    station.line_order,
                    station.line_label,
                    station.station_id,
                    station.station_slug,
                    station.station_name,
                    station.source_key,
                ]
            )

    lines = [f"# {network_name} Rail Stations", ""]
    current_line: Optional[Tuple[int, str]] = None
    for station in stations:
        line_key = (station.line_order, station.line_label)
        if line_key != current_line:
            if current_line is not None:
                lines.append("")
            lines.append(f"## {station.line_label}")
            current_line = line_key
        lines.append(f"- {station.station_name} ({station.station_id})")
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8-sig")


def write_station_resolution(stations: Sequence[Station], resolved: Dict[str, ResolvedStation], output_dir: Path) -> None:
    csv_path = output_dir / "amap_station_matches.csv"
    md_path = output_dir / "amap_station_matches.md"

    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "line_order",
                "line_label",
                "station_id",
                "station_slug",
                "station_name",
                "status",
                "score",
                "query_text",
                "poi_name",
                "poi_type",
                "poi_address",
                "poi_id",
                "location",
                "note",
                "source_key",
            ]
        )
        for station in stations:
            record = resolved.get(station.station_id)
            if record is None:
                writer.writerow(
                    [
                        station.line_order,
                        station.line_label,
                        station.station_id,
                        station.station_slug,
                        station.station_name,
                        "missing",
                        0,
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        station.source_key,
                    ]
                )
                continue

            writer.writerow(
                [
                    record.line_order,
                    record.line_label,
                    record.station_id,
                    record.station_slug,
                    record.station_name,
                    record.status,
                    record.score,
                    record.query_text,
                    record.poi_name,
                    record.poi_type,
                    record.poi_address,
                    record.poi_id,
                    record.location,
                    record.note,
                    record.source_key,
                ]
            )

    lines = [
        "# AMap Station Resolution",
        "",
        "| Line | Station | Status | Score | Matched POI | Location | Note |",
        "|---|---|---|---:|---|---|---|",
    ]
    for station in stations:
        record = resolved.get(station.station_id)
        if record is None:
            lines.append(f"| {station.line_label} | {station.station_name} ({station.station_id}) | missing | 0 |  |  |  |")
            continue
        lines.append(
            f"| {record.line_label} | {record.station_name} ({record.station_id}) | {record.status} | {record.score} | {record.poi_name} | {record.location} | {record.note} |"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8-sig")


def write_route_outputs(
    stations: Sequence[Station],
    routes: Dict[Tuple[str, str], RouteResult],
    output_dir: Path,
    network_name: str,
) -> None:
    matrix_csv = output_dir / "travel_time_matrix.csv"
    pairs_md = output_dir / "travel_time_pairs.md"

    with matrix_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "from_id",
                "from_line_order",
                "from_line_label",
                "from_name",
                "to_id",
                "to_line_order",
                "to_line_label",
                "to_name",
                "status",
                "duration_seconds",
                "duration_minutes",
                "summary",
                "reason",
            ]
        )
        for origin in stations:
            for destination in stations:
                if origin.station_id == destination.station_id:
                    writer.writerow(
                        [
                            origin.station_id,
                            origin.line_order,
                            origin.line_label,
                            origin.station_name,
                            destination.station_id,
                            destination.line_order,
                            destination.line_label,
                            destination.station_name,
                            "self",
                            0,
                            "0.0000",
                            "",
                            "",
                        ]
                    )
                    continue

                result = routes.get((origin.station_id, destination.station_id))
                duration_seconds = result.duration_seconds if result else None
                duration_minutes = f"{duration_seconds / 60:.4f}" if isinstance(duration_seconds, int) else ""
                writer.writerow(
                    [
                        origin.station_id,
                        origin.line_order,
                        origin.line_label,
                        origin.station_name,
                        destination.station_id,
                        destination.line_order,
                        destination.line_label,
                        destination.station_name,
                        result.status if result else "missing",
                        duration_seconds if duration_seconds is not None else "",
                        duration_minutes,
                        result.summary if result else "",
                        result.reason if result else "",
                    ]
                )

    lines = [
        f"# Directed {network_name} Rail Travel Time Pairs",
        "",
        "| From | To | Status | Minutes | Summary |",
        "|---|---|---|---:|---|",
    ]
    for origin in stations:
        for destination in stations:
            if origin.station_id == destination.station_id:
                continue
            result = routes.get((origin.station_id, destination.station_id))
            minutes = ""
            status = "missing"
            summary = ""
            if result is not None:
                status = result.status
                if isinstance(result.duration_seconds, int):
                    minutes = f"{result.duration_seconds / 60:.4f}"
                summary = result.summary
            lines.append(
                f"| {origin.line_label} {origin.station_name} ({origin.station_id}) | {destination.line_label} {destination.station_name} ({destination.station_id}) | {status} | {minutes} | {summary} |"
            )
    pairs_md.write_text("\n".join(lines), encoding="utf-8-sig")


def write_average_ranking(stations: Sequence[Station], routes: Dict[Tuple[str, str], RouteResult], output_dir: Path) -> None:
    ranking_csv = output_dir / "average_time_ranking.csv"
    ranking_md = output_dir / "average_time_ranking.md"
    ranking: List[Tuple[str, int, str, str, float, int]] = []

    for origin in stations:
        values = [
            result.duration_seconds / 60
            for destination in stations
            if origin.station_id != destination.station_id
            for result in [routes.get((origin.station_id, destination.station_id))]
            if result is not None and result.status == "done" and isinstance(result.duration_seconds, int)
        ]
        average_minutes = sum(values) / len(values) if values else math.nan
        ranking.append(
            (
                origin.station_id,
                origin.line_order,
                origin.line_label,
                origin.station_name,
                average_minutes,
                len(values),
            )
        )

    ranking.sort(key=lambda item: (math.inf if math.isnan(item[4]) else item[4], item[1], item[0]))

    with ranking_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["rank", "station_id", "line_order", "line_label", "station_name", "average_minutes", "sample_size"])
        for index, (station_id, line_order, line_label, station_name, average_minutes, sample_size) in enumerate(ranking, start=1):
            writer.writerow(
                [
                    index,
                    station_id,
                    line_order,
                    line_label,
                    station_name,
                    f"{average_minutes:.4f}" if not math.isnan(average_minutes) else "NaN",
                    sample_size,
                ]
            )

    lines = [
        "# Average Travel Time Ranking",
        "",
        "| Rank | Station | Average Minutes | Sample Size |",
        "|---:|---|---:|---:|",
    ]
    for index, (station_id, _, line_label, station_name, average_minutes, sample_size) in enumerate(ranking, start=1):
        avg_text = f"{average_minutes:.4f}" if not math.isnan(average_minutes) else "NaN"
        lines.append(f"| {index} | {line_label} {station_name} ({station_id}) | {avg_text} | {sample_size} |")
    ranking_md.write_text("\n".join(lines), encoding="utf-8-sig")