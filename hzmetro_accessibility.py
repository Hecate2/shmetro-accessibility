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
    dedupe_strings,
    expand_station_name_variants as _base_expand_station_name_variants,
    make_poi_type_score,
    run_city_accessibility_main,
)

HANGZHOU_CITY_CODE = "0571"
HANGZHOU_ADCODE = "330100"
SHAOXING_CITY_CODE = "0575"
SHAOXING_ADCODE = "330600"
NINGBO_CITY_CODE = "0574"
NINGBO_ADCODE = "330200"
JIAOXING_CITY_CODE = "0573"
JIAOXING_ADCODE = "330400"
HAITING_CITY_CODE = "0573"
HAITING_ADCODE = "330481"
SPECIAL_RAIL_KEYWORDS = ("空轨", "磁浮", "磁悬浮", "云巴", "轻轨", "有轨电车", "单轨")
SPECIAL_RAIL_POI_KEYWORDS = SPECIAL_RAIL_KEYWORDS + ("电车站",)
EXTERNAL_LINE_KEYWORDS = ("绍兴", "杭海城际")


def station_uses_special_rail(station: Station) -> bool:
    combined = f"{station.line_label}{station.station_name}"
    return any(keyword in combined for keyword in SPECIAL_RAIL_KEYWORDS)


def station_uses_external_context(station: Station) -> bool:
    return any(keyword in station.line_label for keyword in EXTERNAL_LINE_KEYWORDS)


def expand_station_name_variants(station_name: str) -> List[str]:
    variants = list(_base_expand_station_name_variants(station_name, strip_city_names=["杭州"]))

    if "火车东站" in station_name:
        variants.append(station_name.replace("火车东站", "东站"))

    if "火车南站" in station_name:
        variants.append(station_name.replace("火车南站", "南站"))

    if "火车西站" in station_name:
        variants.append(station_name.replace("火车西站", "西站"))

    return dedupe_strings(variants)


def station_city_names(station: Station) -> List[str]:
    cities = ["杭州"]
    if station_uses_external_context(station):
        if "杭海城际" in station.line_label:
            cities.extend(["海宁", "嘉兴"])
        if "绍兴" in station.line_label:
            cities.append("绍兴")
    return cities


def choose_station_queries(station: Station) -> List[str]:
    return build_station_queries_with_simplified_lines(
        station,
        city_names=station_city_names(station),
        station_uses_special_rail=station_uses_special_rail,
        expand_variants_fn=expand_station_name_variants,
        line_label_simplifiers=[("(支线)", ""), ("有轨电车", "")],
    )


def choose_station_regions(station: Station) -> List[str]:
    regions = [HANGZHOU_ADCODE]
    if station_uses_external_context(station):
        if "杭海城际" in station.line_label:
            regions.extend([HAITING_ADCODE, JIAOXING_ADCODE])
        if "绍兴" in station.line_label:
            regions.append(SHAOXING_ADCODE)
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
        station_name_variants_fn=expand_station_name_variants,
        city_names=station_city_names(station),
        city_adcodes=choose_station_regions(station),
        poi_type_score_fn=poi_type_score,
        special_rail_predicate=station_uses_special_rail,
        special_rail_bonus_keywords=SPECIAL_RAIL_POI_KEYWORDS,
        city_reason="city-match",
    )


def station_city_code(station: Station) -> str:
    if station_uses_external_context(station):
        if "杭海城际" in station.line_label:
            return HAITING_CITY_CODE
        if "绍兴" in station.line_label:
            return SHAOXING_CITY_CODE
    return HANGZHOU_CITY_CODE


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
)


def parse_args() -> argparse.Namespace:
    return build_standard_parser(
        description="Hangzhou rail accessibility crawler backed by AMap APIs",
        default_output="output/hangzhou",
        default_stations_html="杭州地铁车站列表 - 地铁通 MetroMan.html",
        default_db_path="output/hangzhou/amap_transit.db",
        default_service_date_value=default_service_date(),
    ).parse_args()


async def main() -> None:
    args = parse_args()
    await run_city_accessibility_main(args, RESOLVE_RULES, "Hangzhou")


if __name__ == "__main__":
    asyncio.run(main())