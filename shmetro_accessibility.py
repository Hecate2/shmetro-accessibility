#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiosqlite
import httpx
from tqdm import tqdm

AMAP_POI_URL = "https://restapi.amap.com/v3/place/text"
AMAP_TRANSIT_URL = "https://restapi.amap.com/v5/direction/transit/integrated"
SHANGHAI_CITY_CODE = "021"
MAGLEV_KEYWORDS = ("磁浮", "磁悬浮")
RETRIABLE_INFOS = {
    "CUQPS_HAS_EXCEEDED_THE_LIMIT",
    "DAILY_QUERY_OVER_LIMIT",
    "ACCESS_TOO_FREQUENT",
    "SYSTEM_ERROR",
}
LINE_LABELS = {
    41: "浦江线",
    51: "市域机场线",
}


@dataclass(frozen=True)
class Station:
    station_id: str
    station_name: str
    line: int

    @property
    def line_label(self) -> str:
        if self.line in LINE_LABELS:
            return LINE_LABELS[self.line]
        return f"{self.line}号线"


@dataclass(frozen=True)
class ResolvedStation:
    station_id: str
    station_name: str
    line: int
    line_label: str
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
class RouteResult:
    from_id: str
    to_id: str
    status: str
    duration_seconds: Optional[int]
    transit_index: Optional[int]
    summary: str
    reason: str


class AsyncQPSLimiter:
    def __init__(self, qps: float) -> None:
        if qps <= 0:
            raise ValueError("qps must be > 0")
        self.min_interval = 1.0 / qps
        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            wait_seconds = max(0.0, self._next_allowed_at - now)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
                now = loop.time()
            self._next_allowed_at = now + self.min_interval


def default_service_date() -> str:
    today = date.today()
    if today.weekday() < 5:
        return today.isoformat()
    delta = 7 - today.weekday()
    return (today + timedelta(days=delta)).isoformat()


def load_env_file(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip('"').strip("'")
    merged = dict(env)
    for key, value in os.environ.items():
        if key not in merged:
            merged[key] = value
    return merged


def normalize_text(value: str) -> str:
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", value.lower())


def load_station_catalog_from_csv(csv_path: Path) -> List[Station]:
    stations: List[Station] = []
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stations.append(
                Station(
                    station_id=row["station_id"],
                    station_name=row["station_name"],
                    line=int(row["line"]),
                )
            )
    return stations


def choose_station_queries(station: Station) -> List[str]:
    return [
        f"{station.station_name} {station.line_label} 地铁站",
        f"{station.station_name}地铁站 {station.line_label}",
        f"{station.station_name} {station.line_label}",
        f"{station.station_name} 地铁站",
    ]


def candidate_score(station: Station, poi: Dict[str, Any]) -> Tuple[int, str]:
    name = str(poi.get("name") or "")
    address = str(poi.get("address") or "")
    poi_type = str(poi.get("type") or "")
    name_norm = normalize_text(name)
    address_norm = normalize_text(address)
    station_norm = normalize_text(station.station_name)
    line_norm = normalize_text(station.line_label)
    score = 0
    reasons: List[str] = []

    if station_norm and station_norm in name_norm:
        score += 60
        reasons.append("name")
    elif station_norm and station_norm in address_norm:
        score += 30
        reasons.append("address")
    else:
        return -1, "station-name-mismatch"

    if line_norm and line_norm in f"{name_norm}{address_norm}":
        score += 35
        reasons.append("line")

    if "交通设施服务;地铁站;出入口" in poi_type:
        score += 45
        reasons.append("exit")
    elif "交通设施服务;地铁站;地铁站" in poi_type:
        score += 30
        reasons.append("station")
    elif "地铁站" in poi_type:
        score += 15
        reasons.append("subway-type")

    if "出入口" in name:
        score += 10
        reasons.append("exit-name")

    if station.line in LINE_LABELS and line_norm not in f"{name_norm}{address_norm}":
        score -= 10

    return score, ",".join(reasons)


def parse_duration_seconds(raw_value: Any) -> Optional[int]:
    if raw_value is None or raw_value == "":
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def summarize_transit(transit: Dict[str, Any]) -> str:
    labels: List[str] = []
    for segment in transit.get("segments") or []:
        for busline in ((segment.get("bus") or {}).get("buslines") or []):
            name = str(busline.get("name") or "").strip()
            if name:
                labels.append(name)
        railway = segment.get("railway") or {}
        railway_name = str(railway.get("name") or "").strip()
        if railway_name:
            labels.append(railway_name)
    return " | ".join(labels)


def transit_has_forbidden_mode(transit: Dict[str, Any]) -> Optional[str]:
    for segment in transit.get("segments") or []:
        if segment.get("taxi"):
            return "contains_taxi"
        for busline in ((segment.get("bus") or {}).get("buslines") or []):
            text = json.dumps(busline, ensure_ascii=False)
            if any(keyword in text for keyword in MAGLEV_KEYWORDS):
                return "contains_maglev"
        railway = segment.get("railway") or {}
        if railway and any(keyword in json.dumps(railway, ensure_ascii=False) for keyword in MAGLEV_KEYWORDS):
            return "contains_maglev"
    return None


def select_transit(route_payload: Dict[str, Any], from_id: str, to_id: str) -> RouteResult:
    transits = (route_payload.get("route") or {}).get("transits") or []
    if not transits:
        return RouteResult(from_id, to_id, "no_valid_route", None, None, "", "no_transits")

    last_reason = "no_valid_transit"
    for index, transit in enumerate(transits):
        forbidden_reason = transit_has_forbidden_mode(transit)
        if forbidden_reason:
            last_reason = forbidden_reason
            continue
        duration_seconds = parse_duration_seconds((transit.get("cost") or {}).get("duration"))
        if duration_seconds is None:
            last_reason = "missing_duration"
            continue
        return RouteResult(
            from_id=from_id,
            to_id=to_id,
            status="done",
            duration_seconds=duration_seconds,
            transit_index=index,
            summary=summarize_transit(transit),
            reason="ok",
        )
    return RouteResult(from_id, to_id, "no_valid_route", None, None, "", last_reason)


class AMapClient:
    def __init__(
        self,
        key: str,
        secret: str,
        pause_sec: float = 0.0,
        timeout_sec: int = 20,
        retries: int = 4,
        station_search_qps: float = 3.1,
        route_plan_qps: float = 3.1,
    ) -> None:
        self.key = key
        self.secret = secret
        self.pause_sec = pause_sec
        self.timeout_sec = timeout_sec
        self.retries = retries
        self.client = httpx.AsyncClient(timeout=self.timeout_sec)
        self.station_search_limiter = AsyncQPSLimiter(station_search_qps)
        self.route_plan_limiter = AsyncQPSLimiter(route_plan_qps)

    async def aclose(self) -> None:
        await self.client.aclose()

    def sign_params(self, params: Dict[str, Any]) -> str:
        raw = "&".join(f"{key}={params[key]}" for key in sorted(params)) + self.secret
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    async def _request_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        signed_params = {key: str(value) for key, value in params.items() if value is not None}
        signed_params["key"] = self.key
        signed_params["sig"] = self.sign_params(signed_params)

        last_error: Optional[str] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = await self.client.get(url, params=signed_params)
                response.raise_for_status()
                payload = response.json()
            except (httpx.HTTPError, json.JSONDecodeError, ValueError) as exc:
                last_error = str(exc)
                if attempt == self.retries:
                    raise RuntimeError(f"HTTP request failed after {self.retries} attempts: {url}; error={last_error}") from exc
                await asyncio.sleep(min(5.0, attempt * 0.8))
                continue

            status = str(payload.get("status") or "")
            info = str(payload.get("info") or "")
            if status == "1":
                if self.pause_sec > 0:
                    await asyncio.sleep(self.pause_sec)
                return payload

            last_error = f"{info} ({payload.get('infocode')})"
            if info in RETRIABLE_INFOS or "QPS" in info:
                await asyncio.sleep(min(8.0, attempt * 1.2))
                continue
            raise RuntimeError(f"AMap request failed: {url}; info={info}; infocode={payload.get('infocode')}")

        raise RuntimeError(f"AMap request failed after retries: {url}; error={last_error}")

    async def search_station_candidates(self, query_text: str) -> List[Dict[str, Any]]:
        await self.station_search_limiter.acquire()
        payload = await self._request_json(
            AMAP_POI_URL,
            {
                "keywords": query_text,
                "city": "上海",
                "citylimit": "true",
                "extensions": "all",
                "offset": "10",
                "page": "1",
                "output": "JSON",
            },
        )
        return list(payload.get("pois") or [])

    async def route_transit(self, origin: str, destination: str, service_date: str, service_time: str, strategy: str) -> Dict[str, Any]:
        await self.route_plan_limiter.acquire()
        return await self._request_json(
            AMAP_TRANSIT_URL,
            {
                "origin": origin,
                "destination": destination,
                "city1": SHANGHAI_CITY_CODE,
                "city2": SHANGHAI_CITY_CODE,
                "strategy": strategy,
                "AlternativeRoute": "3",
                "nightflag": "0",
                "max_trans": "4",
                "date": service_date,
                "time": service_time,
                "show_fields": "cost",
                "output": "JSON",
            },
        )


async def init_db(db_path: Path) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(str(db_path), timeout=30, isolation_level=None)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await conn.execute("PRAGMA temp_store=MEMORY;")
    await conn.execute("PRAGMA busy_timeout=5000;")
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS station_amap (
            station_id TEXT PRIMARY KEY,
            line INTEGER NOT NULL,
            station_name TEXT NOT NULL,
            line_label TEXT NOT NULL,
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
    )
    await conn.execute(
        """
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
    )
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_route_times_status ON route_times(status)")
    return conn


async def load_resolved_stations(conn: aiosqlite.Connection) -> Dict[str, ResolvedStation]:
    cursor = await conn.execute(
        "SELECT station_id, station_name, line, line_label, query_text, poi_id, poi_name, poi_type, poi_address, location, status, score, note FROM station_amap"
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


async def resolve_station_node(client: AMapClient, station: Station) -> ResolvedStation:
    best_record: Optional[ResolvedStation] = None
    for query_text in choose_station_queries(station):
        candidates = await client.search_station_candidates(query_text)
        for poi in candidates:
            score, note = candidate_score(station, poi)
            if score < 0:
                continue
            location = str(poi.get("location") or "")
            if not location:
                continue
            record = ResolvedStation(
                station_id=station.station_id,
                station_name=station.station_name,
                line=station.line,
                line_label=station.line_label,
                query_text=query_text,
                poi_id=str(poi.get("id") or ""),
                poi_name=str(poi.get("name") or ""),
                poi_type=str(poi.get("type") or ""),
                poi_address=str(poi.get("address") or ""),
                location=location,
                status="resolved" if score >= 110 else "fallback",
                score=score,
                note=note,
            )
            if best_record is None or record.score > best_record.score:
                best_record = record
        if best_record is not None and best_record.score >= 110:
            return best_record

    if best_record is not None:
        return best_record

    return ResolvedStation(
        station_id=station.station_id,
        station_name=station.station_name,
        line=station.line,
        line_label=station.line_label,
        query_text=choose_station_queries(station)[0],
        poi_id="",
        poi_name="",
        poi_type="",
        poi_address="",
        location="",
        status="unresolved",
        score=0,
        note="no_candidate",
    )


async def resolve_stations(
    client: AMapClient,
    conn: aiosqlite.Connection,
    stations: Sequence[Station],
    workers: int,
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
        return await resolve_station_node(client, station)

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
                    result = ResolvedStation(
                        station_id=station.station_id,
                        station_name=station.station_name,
                        line=station.line,
                        line_label=station.line_label,
                        query_text=choose_station_queries(station)[0],
                        poi_id="",
                        poi_name="",
                        poi_type="",
                        poi_address="",
                        location="",
                        status="unresolved",
                        score=0,
                        note=f"error:{exc}",
                    )
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO station_amap(
                        station_id, line, station_name, line_label, query_text,
                        poi_id, poi_name, poi_type, poi_address, location,
                        status, score, note
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        result.station_id,
                        result.line,
                        result.station_name,
                        result.line_label,
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
    client: AMapClient,
    conn: aiosqlite.Connection,
    stations: Sequence[Station],
    resolved_stations: Dict[str, ResolvedStation],
    workers: int,
    service_date: str,
    service_time: str,
    strategy: str,
) -> Dict[Tuple[str, str], RouteResult]:
    existing = await load_route_results(conn)
    resolved_ids = {station_id for station_id, record in resolved_stations.items() if record.location}

    pairs: List[Tuple[Station, Station]] = []
    for origin in stations:
        for destination in stations:
            # ignore self-pairs entirely; those are trivial and we never need to query them
            if origin.station_id == destination.station_id:
                continue
            # only attempt routes for which both ends have an AMap location
            if origin.station_id not in resolved_ids or destination.station_id not in resolved_ids:
                continue
            current = existing.get((origin.station_id, destination.station_id))
            # treat both successful results and explicit "no_valid_route" as finished
            # (no_valid_route is often used when origin==destination in previous runs,
            #  so we want to make sure we don't retry those rows)
            if current and current.status in {"done", "no_valid_route"}:
                continue
            pairs.append((origin, destination))

    completed = 0
    for origin in stations:
        for destination in stations:
            if origin.station_id == destination.station_id:
                continue
            if origin.station_id not in resolved_ids or destination.station_id not in resolved_ids:
                continue
            current = existing.get((origin.station_id, destination.station_id))
            if current and current.status in {"done", "no_valid_route"}:
                completed += 1

    total = completed + len(pairs)
    if not pairs:
        return existing

    async def fetch_one(pair: Tuple[Station, Station]) -> RouteResult:
        origin, destination = pair
        payload = await client.route_transit(
            origin=resolved_stations[origin.station_id].location,
            destination=resolved_stations[destination.station_id].location,
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


def write_station_resolution(stations: Sequence[Station], resolved: Dict[str, ResolvedStation], output_dir: Path) -> None:
    csv_path = output_dir / "amap_station_matches.csv"
    md_path = output_dir / "amap_station_matches.md"

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "line",
                "line_label",
                "station_id",
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
            ]
        )
        for station in stations:
            record = resolved.get(station.station_id)
            if record is None:
                writer.writerow([station.line, station.line_label, station.station_id, station.station_name, "missing", 0, "", "", "", "", "", "", ""])
                continue
            writer.writerow(
                [
                    record.line,
                    record.line_label,
                    record.station_id,
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
                ]
            )

    lines = [
        "# AMap Station Resolution",
        "",
        "| Line | Station | Status | Score | Matched POI | Location | Note |",
        "|---:|---|---|---:|---|---|---|",
    ]
    for station in stations:
        record = resolved.get(station.station_id)
        if record is None:
            lines.append(f"| {station.line_label} | {station.station_name} ({station.station_id}) | missing | 0 |  |  |  |")
            continue
        lines.append(
            f"| {record.line_label} | {record.station_name} ({record.station_id}) | {record.status} | {record.score} | {record.poi_name} | {record.location} | {record.note} |"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")


def write_route_outputs(
    stations: Sequence[Station],
    routes: Dict[Tuple[str, str], RouteResult],
    output_dir: Path,
) -> None:
    matrix_csv = output_dir / "travel_time_matrix.csv"
    pairs_md = output_dir / "travel_time_pairs.md"

    with matrix_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "from_id",
                "from_line",
                "from_name",
                "to_id",
                "to_line",
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
                            origin.line_label,
                            origin.station_name,
                            destination.station_id,
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
                        origin.line_label,
                        origin.station_name,
                        destination.station_id,
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
        "# Directed Shanghai Metro Travel Time Pairs",
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
    pairs_md.write_text("\n".join(lines), encoding="utf-8")


def write_average_ranking(stations: Sequence[Station], routes: Dict[Tuple[str, str], RouteResult], output_dir: Path) -> None:
    ranking_csv = output_dir / "average_time_ranking.csv"
    ranking_md = output_dir / "average_time_ranking.md"
    ranking: List[Tuple[str, str, str, float, int]] = []

    for origin in stations:
        values = [
            result.duration_seconds / 60
            for destination in stations
            if origin.station_id != destination.station_id
            for result in [routes.get((origin.station_id, destination.station_id))]
            if result is not None and result.status == "done" and isinstance(result.duration_seconds, int)
        ]
        average_minutes = sum(values) / len(values) if values else math.nan
        ranking.append((origin.station_id, origin.line_label, origin.station_name, average_minutes, len(values)))

    ranking.sort(key=lambda item: (math.inf if math.isnan(item[3]) else item[3], item[0]))

    with ranking_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["rank", "station_id", "line_label", "station_name", "average_minutes", "sample_size"])
        for index, (station_id, line_label, station_name, average_minutes, sample_size) in enumerate(ranking, start=1):
            writer.writerow(
                [
                    index,
                    station_id,
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
    for index, (station_id, line_label, station_name, average_minutes, sample_size) in enumerate(ranking, start=1):
        avg_text = f"{average_minutes:.4f}" if not math.isnan(average_minutes) else "NaN"
        lines.append(f"| {index} | {line_label} {station_name} ({station_id}) | {avg_text} | {sample_size} |")
    ranking_md.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Shanghai metro accessibility crawler backed by AMap APIs")
    parser.add_argument("--output", default="output", help="Output directory")
    parser.add_argument("--stations-csv", default="output/stations_all.csv", help="Station catalog CSV")
    parser.add_argument("--db-path", default="output/amap_transit.db", help="SQLite database path")
    parser.add_argument("--env-file", default=".env", help="Environment file containing KEY and SEC")
    parser.add_argument("--pause", type=float, default=0.0, help="Optional extra delay after successful AMap requests")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--retries", type=int, default=4, help="Retry count for transient AMap errors")
    parser.add_argument("--resolve-workers", type=int, default=2, help="Concurrent workers for station matching")
    parser.add_argument("--route-workers", type=int, default=6, help="Concurrent workers for route crawling")
    parser.add_argument("--station-search-qps", type=float, default=3.1, help="Hard QPS cap for AMap station search requests")
    parser.add_argument("--route-plan-qps", type=float, default=3.1, help="Hard QPS cap for AMap route planning requests")
    parser.add_argument("--date", default=default_service_date(), help="Service date in YYYY-MM-DD, defaults to a workday")
    parser.add_argument("--time", default="7:15", help="Departure time, for example 7:15")
    parser.add_argument("--strategy", default="8", help="AMap transit strategy, default 8 means shortest time")
    parser.add_argument("--resolve-only", action="store_true", help="Only resolve station nodes without crawling routes")
    parser.add_argument("--compute-only", action="store_true", help="Skip network calls and only rebuild outputs from sqlite")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    stations_csv = Path(args.stations_csv)
    if not stations_csv.exists():
        raise RuntimeError(f"Station catalog not found: {stations_csv}")

    env_values = load_env_file(Path(args.env_file))
    key = env_values.get("KEY")
    secret = env_values.get("SEC")
    if not key or not secret:
        raise RuntimeError("Missing KEY or SEC in environment")

    stations = load_station_catalog_from_csv(stations_csv)
    db_path = Path(args.db_path)
    conn = await init_db(db_path)

    client = AMapClient(
        key=key,
        secret=secret,
        pause_sec=args.pause,
        timeout_sec=args.timeout,
        retries=args.retries,
        station_search_qps=args.station_search_qps,
        route_plan_qps=args.route_plan_qps,
    )
    try:
        resolved = await load_resolved_stations(conn)
        routes = await load_route_results(conn)

        if not args.compute_only:
            resolved = await resolve_stations(client, conn, stations, workers=args.resolve_workers)
            write_station_resolution(stations, resolved, output_dir)

            if not args.resolve_only:
                routes = await crawl_routes(
                    client=client,
                    conn=conn,
                    stations=stations,
                    resolved_stations=resolved,
                    workers=args.route_workers,
                    service_date=args.date,
                    service_time=args.time,
                    strategy=args.strategy,
                )

        write_station_resolution(stations, resolved, output_dir)
        write_route_outputs(stations, routes, output_dir)
        write_average_ranking(stations, routes, output_dir)
    finally:
        await client.aclose()
        await conn.close()

    print(f"Done. Output files saved in: {output_dir.resolve()}")
    print(f"SQLite DB: {db_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())