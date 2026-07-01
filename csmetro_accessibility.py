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
    strip_trailing_parenthetical,
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
            queries.extend(build_standard_metro_queries(station_name, city_name, station.line_label))
    return dedupe_strings(queries)


def choose_station_regions(station: Station) -> List[str]:
    return station_adcodes(station)


def choose_station_poi_types(_: Station) -> List[Optional[str]]:
    return [METRO_POI_TYPECODE, None]


poi_type_score = make_poi_type_score(
    special_rail_predicate=station_uses_special_rail,
    special_rail_poi_keywords=SPECIAL_RAIL_KEYWORDS,
)


def candidate_score(station: Station, poi: Dict[str, Any]) -> Tuple[int, str]:
    return build_city_candidate_score(
        station,
        poi,
        station_name_variants_fn=station_name_variants,
        city_names=station_city_names(station),
        city_adcodes=station_adcodes(station),
        poi_type_score_fn=poi_type_score,
        special_rail_predicate=station_uses_special_rail,
        special_rail_bonus_keywords=SPECIAL_RAIL_KEYWORDS,
        city_reason="changsha",
    )


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
    await run_city_accessibility_main(args, RESOLVE_RULES, "Changsha")


if __name__ == "__main__":
    asyncio.run(main())