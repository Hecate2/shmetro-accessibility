#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from typing import Any, Dict, List, Optional, Tuple

from amap_accessibility_common import default_service_date
from metro_accessibility_common import (
    METRO_POI_TYPECODE,
    Station,
    StationResolveRules,
    build_city_candidate_score,
    build_standard_metro_queries,
    build_standard_parser,
    dedupe_strings,
    make_poi_type_score,
    run_city_accessibility_main,
    station_name_variants,
)

XIAMEN_CITY_CODE = "0592"
XIAMEN_ADCODE = "350200"
ZHANGZHOU_CITY_CODE = "0596"
ZHANGZHOU_ADCODE = "350600"
ZHANGZHOU_LINE6_STATIONS = {"角江路", "角美中心", "文圃路", "角海路", "角美东"}


def station_uses_zhangzhou_context(station: Station) -> bool:
    return station.line_label == "6号线" and station.station_name in ZHANGZHOU_LINE6_STATIONS


def station_city_names(station: Station) -> List[str]:
    cities = ["厦门"]
    if station_uses_zhangzhou_context(station):
        cities.append("漳州")
    return cities


def choose_station_queries(station: Station) -> List[str]:
    queries: List[str] = []
    for station_name in station_name_variants(station.station_name):
        for city_name in station_city_names(station):
            queries.extend(build_standard_metro_queries(station_name, city_name, station.line_label))
    return dedupe_strings(queries)


def choose_station_regions(station: Station) -> List[str]:
    regions = [XIAMEN_ADCODE]
    if station_uses_zhangzhou_context(station):
        regions.append(ZHANGZHOU_ADCODE)
    return regions


def choose_station_poi_types(_: Station) -> List[Optional[str]]:
    return [METRO_POI_TYPECODE, None]


poi_type_score = make_poi_type_score()


def candidate_score(station: Station, poi: Dict[str, Any]) -> Tuple[int, str]:
    return build_city_candidate_score(
        station,
        poi,
        station_name_variants_fn=station_name_variants,
        city_names=station_city_names(station),
        city_adcodes=choose_station_regions(station),
        poi_type_score_fn=poi_type_score,
        city_reason="xiamen",
    )


def station_city_code(station: Station) -> str:
    if station_uses_zhangzhou_context(station):
        return ZHANGZHOU_CITY_CODE
    return XIAMEN_CITY_CODE


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
)


def parse_args() -> argparse.Namespace:
    return build_standard_parser(
        description="Xiamen rail accessibility crawler backed by AMap APIs",
        default_output="output/xiamen",
        default_stations_html="厦门地铁车站列表 - 地铁通 MetroMan.html",
        default_db_path="output/xiamen/amap_transit.db",
        default_service_date_value=default_service_date(),
    ).parse_args()


async def main() -> None:
    args = parse_args()
    await run_city_accessibility_main(args, RESOLVE_RULES, "Xiamen")


if __name__ == "__main__":
    asyncio.run(main())
