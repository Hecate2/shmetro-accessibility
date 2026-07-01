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
    build_standard_parser,
    build_station_queries_with_simplified_lines,
    expand_station_name_variants,
    make_poi_type_score,
    run_city_accessibility_main,
)

NANJING_CITY_CODE = "025"
NANJING_ADCODE = "320100"
MAANSHAN_CITY_CODE = "0555"
MAANSHAN_ADCODE = "340500"
ZHENJIANG_CITY_CODE = "0511"
ZHENJIANG_ADCODE = "321100"
JURONG_CITY_CODE = "0511"
JURONG_ADCODE = "321183"
CHUZHOU_CITY_CODE = "0550"
CHUZHOU_ADCODE = "341100"
SPECIAL_RAIL_KEYWORDS = ("空轨", "磁浮", "磁悬浮", "云巴", "轻轨", "有轨电车", "单轨")
SPECIAL_RAIL_POI_KEYWORDS = SPECIAL_RAIL_KEYWORDS + ("电车站",)
EXTERNAL_LINE_KEYWORDS = ("S2号线", "S6号线", "宁滁线")


def station_uses_special_rail(station: Station) -> bool:
    combined = f"{station.line_label}{station.station_name}"
    return any(keyword in combined for keyword in SPECIAL_RAIL_KEYWORDS)


def station_uses_external_context(station: Station) -> bool:
    return any(keyword in station.line_label for keyword in EXTERNAL_LINE_KEYWORDS)


def station_city_names(station: Station) -> List[str]:
    cities = ["南京"]
    if station_uses_external_context(station):
        if "S2号线" in station.line_label:
            cities.append("马鞍山")
        if "S6号线" in station.line_label:
            cities.extend(["句容", "镇江"])
        if "宁滁线" in station.line_label:
            cities.extend(["滁州", "来安"])
    return cities


def choose_station_queries(station: Station) -> List[str]:
    return build_station_queries_with_simplified_lines(
        station,
        city_names=station_city_names(station),
        station_uses_special_rail=station_uses_special_rail,
        expand_variants_fn=lambda name: expand_station_name_variants(name, strip_city_names=["南京"]),
        line_label_simplifiers=[("(支线)", ""), ("有轨电车", "")],
    )


def choose_station_regions(station: Station) -> List[str]:
    regions = [NANJING_ADCODE]
    if station_uses_external_context(station):
        if "S2号线" in station.line_label:
            regions.append(MAANSHAN_ADCODE)
        if "S6号线" in station.line_label:
            regions.extend([JURONG_ADCODE, ZHENJIANG_ADCODE])
        if "宁滁线" in station.line_label:
            regions.append(CHUZHOU_ADCODE)
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
        station_name_variants_fn=lambda name: expand_station_name_variants(name, strip_city_names=["南京"]),
        city_names=station_city_names(station),
        city_adcodes=choose_station_regions(station),
        poi_type_score_fn=poi_type_score,
        special_rail_predicate=station_uses_special_rail,
        special_rail_bonus_keywords=SPECIAL_RAIL_POI_KEYWORDS,
        city_reason="city-match",
    )


def station_city_code(station: Station) -> str:
    if station_uses_external_context(station):
        if "S2号线" in station.line_label:
            return MAANSHAN_CITY_CODE
        if "S6号线" in station.line_label:
            return JURONG_CITY_CODE
        if "宁滁线" in station.line_label:
            return CHUZHOU_CITY_CODE
    return NANJING_CITY_CODE


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
)


def parse_args() -> argparse.Namespace:
    return build_standard_parser(
        description="Nanjing rail accessibility crawler backed by AMap APIs",
        default_output="output/nanjing",
        default_stations_html="南京地铁车站列表 - 地铁通 MetroMan.html",
        default_db_path="output/nanjing/amap_transit.db",
        default_service_date_value=default_service_date(),
    ).parse_args()


async def main() -> None:
    args = parse_args()
    await run_city_accessibility_main(args, RESOLVE_RULES, "Nanjing")


if __name__ == "__main__":
    asyncio.run(main())