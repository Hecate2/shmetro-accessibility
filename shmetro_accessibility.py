#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from amap_accessibility_common import default_service_date, load_amap_credentials, load_env_file, normalize_text
from metro_accessibility_common import (
    METRO_POI_TYPECODE,
    POI_TYPE_STATION,
    MetroAMapClient,
    Station,
    StationResolveRules,
    crawl_routes,
    dedupe_strings,
    init_db,
    load_resolved_stations,
    load_route_results,
    load_station_catalog_with_source,
    resolve_stations,
    sync_station_catalog,
    write_average_ranking,
    write_route_outputs,
    write_station_resolution,
)

SHANGHAI_CITY_CODE = "021"
SHANGHAI_ADCODE = "310000"
SUZHOU_ADCODE = "320500"
SUZHOU_LINE11_STATIONS = {"花桥", "光明路", "兆丰路"}
LINE_LABELS = {
    41: "浦江线",
    51: "市域机场线",
}


def line_label_for_order(line_order: int) -> str:
    if line_order in LINE_LABELS:
        return LINE_LABELS[line_order]
    return f"{line_order}号线"


def load_station_catalog_from_csv(csv_path: Path) -> List[Station]:
    stations: List[Station] = []
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])

        if {"line", "station_id", "station_name"}.issubset(fieldnames):
            for row in reader:
                station_id = row["station_id"]
                line_order = int(row["line"])
                stations.append(
                    Station(
                        station_id=station_id,
                        station_slug=station_id,
                        station_name=row["station_name"],
                        line_order=line_order,
                        line_label=line_label_for_order(line_order),
                        source_key=station_id,
                    )
                )
            return stations

        if {"station_id", "station_slug", "station_name", "line_order", "line_label", "source_key"}.issubset(fieldnames):
            for row in reader:
                stations.append(
                    Station(
                        station_id=row["station_id"],
                        station_slug=row["station_slug"],
                        station_name=row["station_name"],
                        line_order=int(row["line_order"]),
                        line_label=row["line_label"],
                        source_key=row["source_key"],
                    )
                )
            return stations

    raise RuntimeError(f"Unsupported station CSV format: {csv_path}")


def write_station_catalog_csv(stations: Sequence[Station], output_dir: Path) -> None:
    csv_path = output_dir / "stations_all.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["line", "station_id", "station_name"])
        for station in stations:
            writer.writerow([station.line_order, station.station_id, station.station_name])


def choose_station_queries(station: Station) -> List[str]:
    return dedupe_strings(
        [
            f"{station.station_name} 上海 {station.line_label} 地铁站",
            f"上海 {station.station_name} {station.line_label} 地铁站",
            f"{station.station_name} {station.line_label} 地铁站",
            f"{station.line_label} {station.station_name} 上海 地铁站",
            f"上海地铁 {station.line_label} {station.station_name}",
        ]
    )


def choose_station_regions(station: Station) -> List[str]:
    regions = [SHANGHAI_ADCODE]
    if station.line_order == 11 and station.station_name in SUZHOU_LINE11_STATIONS:
        regions.append(SUZHOU_ADCODE)
    return regions


def choose_station_poi_types(_: Station) -> List[Optional[str]]:
    return [METRO_POI_TYPECODE]


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

    if poi_type == POI_TYPE_STATION:
        score += 50
        reasons.append("station")
    else:
        return -1, "unsupported-poi-type"

    if station.line_order in LINE_LABELS and line_norm not in f"{name_norm}{address_norm}":
        score -= 10

    return score, ",".join(reasons)


def station_city_code(_: Station) -> str:
    return SHANGHAI_CITY_CODE


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
)


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
    parser.add_argument("--station-search-qps", type=float, default=3.01, help="Hard QPS cap for AMap station search requests")
    parser.add_argument("--route-plan-qps", type=float, default=3.01, help="Hard QPS cap for AMap route planning requests")
    parser.add_argument("--date", default=default_service_date(), help="Service date in YYYY-MM-DD, defaults to a workday")
    parser.add_argument("--time", default="7:15", help="Departure time, for example 7:15")
    parser.add_argument("--strategy", default="0", help="AMap transit strategy, default 0 is the auto-recommended route")
    parser.add_argument("--resolve-only", action="store_true", help="Only resolve station nodes without crawling routes")
    parser.add_argument("--compute-only", action="store_true", help="Skip network calls and only rebuild outputs from sqlite")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await init_db(db_path)

    client: Optional[MetroAMapClient] = None
    try:
        stations_csv = Path(args.stations_csv)
        if stations_csv.exists():
            stations = load_station_catalog_from_csv(stations_csv)
            await sync_station_catalog(conn, stations)
        else:
            catalog = await load_station_catalog_with_source(conn)
            stations = catalog.stations
            print(f"Station CSV not found: {stations_csv}. Loaded {len(stations)} stations from {catalog.source} in {db_path}.")

        write_station_catalog_csv(stations, output_dir)

        resolved = await load_resolved_stations(conn)
        routes = await load_route_results(conn)

        if not args.compute_only:
            env_values = load_env_file(Path(args.env_file))
            credentials = load_amap_credentials(env_values)
            client = MetroAMapClient(
                credentials=credentials,
                pause_sec=args.pause,
                timeout_sec=args.timeout,
                retries=args.retries,
                station_search_qps=args.station_search_qps,
                route_plan_qps=args.route_plan_qps,
                search_page_size=10,
            )

            resolved = await resolve_stations(client, conn, stations, workers=args.resolve_workers, rules=RESOLVE_RULES)
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
                    route_city_code=RESOLVE_RULES.route_city_code,
                )

        write_station_catalog_csv(stations, output_dir)
        write_station_resolution(stations, resolved, output_dir)
        write_route_outputs(stations, routes, output_dir, "Shanghai Metro")
        write_average_ranking(stations, routes, output_dir)
    finally:
        if client is not None:
            await client.aclose()
        await conn.close()

    print(f"Done. Output files saved in: {output_dir.resolve()}")
    print(f"SQLite DB: {db_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())