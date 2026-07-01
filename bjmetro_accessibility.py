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

BEIJING_CITY_CODE = "010"
BEIJING_ADCODE = "110000"
# S1线 is the maglev line; 西郊线 and 亦庄T1线 are tram-like lines. None of
# these labels contain the generic special-rail keywords used below, so they
# need explicit identification.
SPECIAL_RAIL_LINE_LABELS = frozenset({"S1线", "西郊线", "亦庄T1线"})
SPECIAL_RAIL_KEYWORDS = ("磁浮", "磁悬浮", "有轨电车", "轻轨", "云巴", "单轨")
SPECIAL_RAIL_POI_KEYWORDS = SPECIAL_RAIL_KEYWORDS + ("电车站",)


def station_uses_special_rail(station: Station) -> bool:
    if station.line_label in SPECIAL_RAIL_LINE_LABELS:
        return True
    combined = f"{station.line_label}{station.station_name}"
    return any(keyword in combined for keyword in SPECIAL_RAIL_KEYWORDS)


def station_uses_maglev(station: Station) -> bool:
    """S1线 uses magnetic levitation technology."""
    return station.line_label == "S1线" or any(
        kw in f"{station.line_label}{station.station_name}" for kw in ("磁浮", "磁悬浮")
    )


def station_city_names(_: Station) -> List[str]:
    return ["北京"]


def choose_station_queries(station: Station) -> List[str]:
    queries: List[str] = []
    for station_name in station_name_variants(station.station_name):
        if station_uses_special_rail(station):
            if station_uses_maglev(station):
                queries.extend(
                    [
                        f"{station_name} 北京 {station.line_label} 站",
                        f"北京 {station_name} {station.line_label} 站",
                        f"{station_name} 北京 磁悬浮 站",
                        f"北京磁悬浮 {station.line_label} {station_name}",
                        f"{station_name} 北京 轨道站",
                    ]
                )
            else:
                # 西郊线 — tram line
                queries.extend(
                    [
                        f"{station_name} 北京 {station.line_label} 站",
                        f"北京 {station_name} {station.line_label} 站",
                        f"{station_name} 北京 有轨电车 站",
                        f"北京有轨电车 {station.line_label} {station_name}",
                        f"{station_name} 北京 电车站",
                    ]
                )
        queries.extend(build_standard_metro_queries(station_name, "北京", station.line_label))
    return dedupe_strings(queries)


def choose_station_regions(_: Station) -> List[str]:
    return [BEIJING_ADCODE]


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
        city_reason="beijing",
    )


def station_city_code(_: Station) -> str:
    return BEIJING_CITY_CODE


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
)


def parse_args() -> argparse.Namespace:
    return build_standard_parser(
        description="Beijing rail accessibility crawler backed by AMap APIs",
        default_output="output/beijing",
        default_stations_html="北京地铁车站列表 - 地铁通 MetroMan.html",
        default_db_path="output/beijing/amap_transit.db",
        default_service_date_value=default_service_date(),
    ).parse_args()


async def main() -> None:
    args = parse_args()
    await run_city_accessibility_main(args, RESOLVE_RULES, "Beijing")


if __name__ == "__main__":
    asyncio.run(main())
