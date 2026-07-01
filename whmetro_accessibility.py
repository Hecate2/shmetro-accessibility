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

WUHAN_CITY_CODE = "027"
WUHAN_ADCODE = "420100"
EZHOU_CITY_CODE = "0711"
EZHOU_ADCODE = "420700"
EZHOU_LINE11_STATIONS = {"葛店南站"}
SPECIAL_RAIL_KEYWORDS = ("空轨", "磁浮", "磁悬浮", "云巴", "轻轨", "有轨电车", "单轨")
SPECIAL_RAIL_POI_KEYWORDS = SPECIAL_RAIL_KEYWORDS + ("电车站",)


def station_uses_special_rail(station: Station) -> bool:
    combined = f"{station.line_label}{station.station_name}"
    return any(keyword in combined for keyword in SPECIAL_RAIL_KEYWORDS)


def station_uses_ezhou_context(station: Station) -> bool:
    return station.line_label == "11号线" and station.station_name in EZHOU_LINE11_STATIONS


def station_city_names(station: Station) -> List[str]:
    cities = ["武汉"]
    if station_uses_ezhou_context(station):
        cities.append("鄂州")
    return cities


def choose_station_queries(station: Station) -> List[str]:
    queries: List[str] = []
    for station_name in station_name_variants(station.station_name):
        for city_name in station_city_names(station):
            if station_uses_special_rail(station):
                queries.extend(
                    [
                        f"{station_name} {city_name} {station.line_label} 站",
                        f"{city_name} {station_name} {station.line_label} 站",
                        f"{station_name} {city_name} 空轨站",
                        f"{city_name}空轨 {station_name}",
                        f"{station_name} {city_name} 轨道站",
                    ]
                )
            queries.extend(build_standard_metro_queries(station_name, city_name, station.line_label))
    return dedupe_strings(queries)


def choose_station_regions(station: Station) -> List[str]:
    regions = [WUHAN_ADCODE]
    if station_uses_ezhou_context(station):
        regions.append(EZHOU_ADCODE)
    return regions


def choose_station_poi_types(_: Station) -> List[Optional[str]]:
    return [METRO_POI_TYPECODE, None]


poi_type_score = make_poi_type_score(
    special_rail_predicate=station_uses_special_rail,
    special_rail_poi_keywords=SPECIAL_RAIL_POI_KEYWORDS,
)


def candidate_score(station: Station, poi: Dict[str, Any]) -> Tuple[int, str]:
    return build_city_candidate_score(
        station,
        poi,
        station_name_variants_fn=station_name_variants,
        city_names=station_city_names(station),
        city_adcodes=choose_station_regions(station),
        poi_type_score_fn=poi_type_score,
        special_rail_predicate=station_uses_special_rail,
        special_rail_bonus_keywords=SPECIAL_RAIL_POI_KEYWORDS,
        city_reason="wuhan",
    )


def station_city_code(station: Station) -> str:
    if station_uses_ezhou_context(station):
        return EZHOU_CITY_CODE
    return WUHAN_CITY_CODE


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
)


def parse_args() -> argparse.Namespace:
    return build_standard_parser(
        description="Wuhan rail accessibility crawler backed by AMap APIs",
        default_output="output/wuhan",
        default_stations_html="武汉地铁车站列表 - 地铁通 MetroMan.html",
        default_db_path="output/wuhan/amap_transit.db",
        default_service_date_value=default_service_date(),
    ).parse_args()


async def main() -> None:
    args = parse_args()
    await run_city_accessibility_main(args, RESOLVE_RULES, "Wuhan")


if __name__ == "__main__":
    asyncio.run(main())