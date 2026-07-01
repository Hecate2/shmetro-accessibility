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

SHENZHEN_CITY_CODE = "0755"
SHENZHEN_ADCODE = "440300"
SPECIAL_RAIL_KEYWORDS = ("云巴", "轻轨", "有轨电车", "单轨")


def choose_station_queries(station: Station) -> List[str]:
    queries: List[str] = []
    for station_name in station_name_variants(station.station_name):
        if "云巴" in station.line_label:
            queries.extend(
                [
                    f"{station_name} 深圳 {station.line_label} 站",
                    f"深圳 {station_name} {station.line_label} 站",
                    f"{station_name} 深圳 云巴站",
                    f"深圳云巴 {station_name}",
                    f"{station_name} 深圳 轨道站",
                ]
            )
        queries.extend(build_standard_metro_queries(station_name, "深圳", station.line_label))
    return dedupe_strings(queries)


def choose_station_regions(_: Station) -> List[str]:
    return [SHENZHEN_ADCODE]


def choose_station_poi_types(_: Station) -> List[Optional[str]]:
    return [METRO_POI_TYPECODE, None]


poi_type_score = make_poi_type_score(
    special_rail_predicate=lambda station: "云巴" in station.line_label,
    special_rail_poi_keywords=SPECIAL_RAIL_KEYWORDS,
)


def candidate_score(station: Station, poi: Dict[str, Any]) -> Tuple[int, str]:
    return build_city_candidate_score(
        station,
        poi,
        station_name_variants_fn=station_name_variants,
        city_names=["深圳"],
        city_adcodes=[SHENZHEN_ADCODE],
        poi_type_score_fn=poi_type_score,
        special_rail_predicate=lambda station: "云巴" in station.line_label,
        special_rail_bonus_keywords=("云巴",),
        city_reason="shenzhen",
    )


def station_city_code(_: Station) -> str:
    return SHENZHEN_CITY_CODE


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
)


def parse_args() -> argparse.Namespace:
    return build_standard_parser(
        description="Shenzhen rail accessibility crawler backed by AMap APIs",
        default_output="output/shenzhen",
        default_stations_html="深圳地铁车站列表 - 地铁通 MetroMan.html",
        default_db_path="output/shenzhen/amap_transit.db",
        default_service_date_value=default_service_date(),
    ).parse_args()


async def main() -> None:
    args = parse_args()
    await run_city_accessibility_main(args, RESOLVE_RULES, "Shenzhen")


if __name__ == "__main__":
    asyncio.run(main())