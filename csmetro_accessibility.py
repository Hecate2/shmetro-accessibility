#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from amap_accessibility_common import default_service_date, load_amap_credentials, load_env_file, normalize_text
from metro_accessibility_common import (
    METRO_POI_TYPECODE,
    POI_TYPE_STATION,
    MetroAMapClient,
    Station,
    StationResolveRules,
    build_standard_parser,
    crawl_routes,
    dedupe_strings,
    init_db,
    load_or_sync_station_catalog,
    load_resolved_stations,
    load_route_results,
    resolve_stations,
    station_name_variants,
    strip_trailing_parenthetical,
    write_average_ranking,
    write_route_outputs,
    write_station_catalog,
    write_station_resolution,
)

CHANGSHA_CITY_CODE = "0731"
CHANGSHA_ADCODE = "430100"
XIANGTAN_CITY_CODE = "0732"
XIANGTAN_ADCODE = "430300"
XIANGTAN_LINE3_STATIONS = {"船形山", "黄家湾", "湘潭北站"}
SPECIAL_RAIL_KEYWORDS = ("磁浮", "磁悬浮", "云巴", "轻轨", "有轨电车", "单轨")


def station_uses_special_rail(station: Station) -> bool:
    combined = f"{station.line_label}{station.station_name}"
    return any(keyword in combined for keyword in SPECIAL_RAIL_KEYWORDS)


def station_uses_xiangtan_context(station: Station) -> bool:
    return station.line_label == "3号线" and strip_trailing_parenthetical(station.station_name) in XIANGTAN_LINE3_STATIONS


def station_city_names(station: Station) -> List[str]:
    cities = ["长沙"]
    if station_uses_xiangtan_context(station):
        cities.append("湘潭")
    return cities


def station_adcodes(station: Station) -> List[str]:
    adcodes = [CHANGSHA_ADCODE]
    if station_uses_xiangtan_context(station):
        adcodes.append(XIANGTAN_ADCODE)
    return adcodes


def station_city_code(station: Station) -> str:
    if station_uses_xiangtan_context(station):
        return XIANGTAN_CITY_CODE
    return CHANGSHA_CITY_CODE


def choose_station_queries(station: Station) -> List[str]:
    queries: List[str] = []
    for station_name in station_name_variants(station.station_name):
        for city_name in station_city_names(station):
            if station_uses_special_rail(station):
                queries.extend(
                    [
                        f"{station_name} {city_name} {station.line_label} 站",
                        f"{city_name} {station_name} {station.line_label} 站",
                        f"{station_name} {city_name} 磁浮站",
                        f"{city_name}磁浮 {station_name}",
                        f"{station_name} {city_name} 轨道站",
                    ]
                )

            queries.extend(
                [
                    f"{station_name} {city_name} {station.line_label} 地铁站",
                    f"{city_name} {station_name} {station.line_label} 地铁站",
                    f"{station_name} {station.line_label} 地铁站",
                    f"{station_name} {city_name} {station.line_label} 站",
                    f"{city_name}地铁 {station.line_label} {station_name}",
                    f"{station_name} {city_name} 地铁站",
                ]
            )
    return dedupe_strings(queries)


def choose_station_regions(station: Station) -> List[str]:
    return station_adcodes(station)


def choose_station_poi_types(_: Station) -> List[Optional[str]]:
    return [METRO_POI_TYPECODE, None]


def poi_type_score(station: Station, poi_type: str) -> Optional[Tuple[int, str]]:
    if poi_type == POI_TYPE_STATION:
        return 50, "metro-station"

    if station_uses_special_rail(station) and any(keyword in poi_type for keyword in SPECIAL_RAIL_KEYWORDS):
        return 35, "special-rail"

    return None


def candidate_score(station: Station, poi: Dict[str, Any]) -> Tuple[int, str]:
    name = str(poi.get("name") or "")
    address = str(poi.get("address") or "")
    poi_type = str(poi.get("type") or "")
    city_name = str(poi.get("cityname") or "")
    district_name = str(poi.get("adname") or "")
    adcode = str(poi.get("adcode") or "")

    station_name_norms = [normalize_text(value) for value in station_name_variants(station.station_name)]
    line_norm = normalize_text(station.line_label)
    name_norm = normalize_text(name)
    address_norm = normalize_text(address)
    combined_norm = normalize_text(f"{name} {address} {city_name} {district_name}")

    score = 0
    reasons: List[str] = []

    matched_station_norm = next(
        (
            station_norm
            for station_norm in station_name_norms
            if station_norm and (station_norm == name_norm or station_norm in name_norm)
        ),
        None,
    )
    if matched_station_norm is not None:
        score += 60
        reasons.append("name")
        if name_norm in {matched_station_norm, f"{matched_station_norm}站"}:
            score += 15
            reasons.append("exact-name")
    elif any(station_norm and station_norm in address_norm for station_norm in station_name_norms):
        score += 30
        reasons.append("address")
    else:
        return -1, "station-name-mismatch"

    if line_norm and line_norm in combined_norm:
        score += 35
        reasons.append("line")

    if adcode in station_adcodes(station) or any(city in f"{city_name}{district_name}{address}" for city in station_city_names(station)):
        score += 10
        reasons.append("changsha")

    type_result = poi_type_score(station, poi_type)
    if type_result is None:
        return -1, "unsupported-poi-type"

    score += type_result[0]
    reasons.append(type_result[1])

    if station_uses_special_rail(station) and any(keyword in f"{name}{address}{poi_type}" for keyword in SPECIAL_RAIL_KEYWORDS):
        score += 15
        reasons.append("special-rail-keyword")

    return score, ",".join(reasons)


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
)


def parse_args() -> argparse.Namespace:
    return build_standard_parser(
        description="Changsha rail accessibility crawler backed by AMap APIs",
        default_output="output/changsha",
        default_stations_html="长沙地铁车站列表 - 地铁通 MetroMan.html",
        default_db_path="output/changsha/amap_transit.db",
        default_service_date_value=default_service_date(),
    ).parse_args()


async def main() -> None:
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await init_db(db_path)

    client: Optional[MetroAMapClient] = None
    try:
        stations_html = Path(args.stations_html)
        catalog = await load_or_sync_station_catalog(conn, stations_html)
        stations = catalog.stations
        if catalog.source != "html":
            print(f"Station HTML not found: {stations_html}. Loaded {len(stations)} stations from {catalog.source} in {db_path}.")

        write_station_catalog(stations, output_dir, "Changsha")

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
                search_page_size=25,
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

        write_station_catalog(stations, output_dir, "Changsha")
        write_station_resolution(stations, resolved, output_dir)
        write_route_outputs(stations, routes, output_dir, "Changsha")
        write_average_ranking(stations, routes, output_dir)
    finally:
        if client is not None:
            await client.aclose()
        await conn.close()

    print(f"Done. Output files saved in: {output_dir.resolve()}")
    print(f"SQLite DB: {db_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())