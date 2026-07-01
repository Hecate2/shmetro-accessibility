#!/usr/bin/env python3
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple

from amap_accessibility_common import default_service_date
from metro_accessibility_common import (
    METRO_POI_TYPECODE,
    ResolvedStation,
    Station,
    StationResolveRules,
    build_city_candidate_score,
    build_standard_parser,
    build_station_queries_with_simplified_lines,
    expand_station_name_variants,
    make_poi_type_score,
    run_city_accessibility_main,
)

CHONGQING_CITY_CODE = "023"
CHONGQING_ADCODE = "500100"
YUNBA_LINE_LABEL = "重庆云巴"
SPECIAL_RAIL_KEYWORDS = ("空轨", "磁浮", "磁悬浮", "云巴", "轻轨", "有轨电车", "单轨", "重庆云巴")
SPECIAL_RAIL_POI_KEYWORDS = SPECIAL_RAIL_KEYWORDS + ("电车站",)


def station_uses_special_rail(station: Station) -> bool:
    combined = f"{station.line_label}{station.station_name}"
    return any(keyword in combined for keyword in SPECIAL_RAIL_KEYWORDS)


def station_is_yunba(station: Station) -> bool:
    return YUNBA_LINE_LABEL in station.line_label


def station_city_names(_: Station) -> List[str]:
    return ["重庆"]


def choose_station_queries(station: Station) -> List[str]:
    return build_station_queries_with_simplified_lines(
        station,
        city_names=station_city_names(station),
        station_uses_special_rail=station_uses_special_rail,
        expand_variants_fn=lambda name: expand_station_name_variants(name, strip_city_names=["重庆"]),
        line_label_simplifiers=[("(支线)", ""), ("有轨电车", "")],
    )


def choose_station_regions(_: Station) -> List[str]:
    return [CHONGQING_ADCODE]


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
        station_name_variants_fn=lambda name: expand_station_name_variants(name, strip_city_names=["重庆"]),
        city_names=station_city_names(station),
        city_adcodes=choose_station_regions(station),
        poi_type_score_fn=poi_type_score,
        special_rail_predicate=station_uses_special_rail,
        special_rail_bonus_keywords=SPECIAL_RAIL_POI_KEYWORDS,
        city_reason="chongqing",
    )


def station_city_code(_: Station) -> str:
    return CHONGQING_CITY_CODE


# --- 重庆云巴 geocode fallback -------------------------------------------------

YUNBA_BISHAN_BOUNDS = (106.1, 106.35, 29.45, 29.7)


def _parse_location(loc: str) -> Optional[Tuple[float, float]]:
    try:
        lng, lat = loc.split(",")
        return float(lng), float(lat)
    except Exception:
        return None


def _is_in_bishan_area(loc: str) -> bool:
    parsed = _parse_location(loc)
    if parsed is None:
        return False
    lng, lat = parsed
    min_lng, max_lng, min_lat, max_lat = YUNBA_BISHAN_BOUNDS
    return min_lng <= lng <= max_lng and min_lat <= lat <= max_lat


def _build_yunba_address_variants(station_name: str) -> List[str]:
    base = station_name.replace("(重庆云巴)", "").replace("重庆云巴", "").strip()
    variants = [
        f"璧山云巴{base}站",
        f"重庆璧山云巴{base}站",
        f"璧山区云巴{base}站",
    ]
    if "公园" in base:
        variants.extend([
            f"璧山{base}云巴站",
            f"璧山区{base}云巴站",
            f"璧山{base}",
            f"璧山区{base}",
        ])
    if base == "1号地铁站":
        variants.extend([
            "璧山云巴1号地铁站",
            "重庆璧山云巴1号地铁站",
            "璧山区云巴1号地铁站",
        ])
    if base == "成渝高铁站":
        variants.extend([
            "璧山云巴成渝高铁站",
            "重庆璧山云巴成渝高铁站",
            "璧山区云巴成渝高铁站",
            "璧山高铁站",
        ])
    if base == "科创小镇":
        variants.extend([
            "璧山科创小镇云巴站",
            "璧山区科创小镇云巴站",
        ])
    if base in ("景山路", "聚金大道", "千层岩"):
        variants.extend([
            f"璧山区{base}云巴站",
            f"璧山{base}云巴站",
            f"璧山区{base}地铁站",
        ])
    return variants


def _score_yunba_geocode(geo: Dict[str, Any]) -> int:
    formatted = str(geo.get("formatted_address") or "")
    level = str(geo.get("level") or "")
    loc = str(geo.get("location") or "")
    score = 0
    if "璧山" in formatted:
        score += 50
    if "云巴" in formatted:
        score += 30
    if "公交站" in formatted or "地铁站" in formatted or "有轨电车站" in formatted:
        score += 20
    if level == "公交地铁站点":
        score += 10
    if _is_in_bishan_area(loc):
        score += 40
    return score


async def _resolve_yunba_by_geocode(client: Any, station: Station) -> Optional[ResolvedStation]:
    if not station_is_yunba(station):
        return None

    addresses = _build_yunba_address_variants(station.station_name)
    best_geo: Optional[Dict[str, Any]] = None
    best_address = ""
    best_score = -1

    for address in addresses:
        try:
            geocodes = await client.geocode(address, city="重庆")
        except Exception:
            geocodes = []
        for geo in geocodes:
            score = _score_yunba_geocode(geo)
            if score > best_score:
                best_score = score
                best_geo = geo
                best_address = address
            if score >= 100:
                break
        if best_score >= 100:
            break
        await asyncio.sleep(0.5)

    if not best_geo:
        return None

    location = str(best_geo.get("location") or "")
    if not location:
        return None

    formatted = str(best_geo.get("formatted_address") or best_address)
    level = str(best_geo.get("level") or "")
    city = str(best_geo.get("city") or "")
    district = str(best_geo.get("district") or "")
    adcode = str(best_geo.get("adcode") or "")

    return ResolvedStation(
        station_id=station.station_id,
        station_slug=station.station_slug,
        station_name=station.station_name,
        line_order=station.line_order,
        line_label=station.line_label,
        source_key=station.source_key,
        query_text=best_address,
        poi_id=f"geocode:{adcode}:{location}",
        poi_name=formatted,
        poi_type="交通设施服务;公交车站;电车站",
        poi_address=f"{city}{district}",
        location=location,
        status="resolved",
        score=max(30, best_score),
        note=f"geocode-level={level}",
    )


RESOLVE_RULES = StationResolveRules(
    choose_queries=choose_station_queries,
    choose_regions=choose_station_regions,
    choose_poi_types=choose_station_poi_types,
    candidate_score=candidate_score,
    route_city_code=station_city_code,
    fallback_resolver=_resolve_yunba_by_geocode,
)


def parse_args() -> argparse.Namespace:
    return build_standard_parser(
        description="Chongqing rail accessibility crawler backed by AMap APIs",
        default_output="output/chongqing",
        default_stations_html="重庆轨道交通车站列表 - 地铁通 MetroMan.html",
        default_db_path="output/chongqing/amap_transit.db",
        default_service_date_value=default_service_date(),
    ).parse_args()


async def main() -> None:
    args = parse_args()
    await run_city_accessibility_main(args, RESOLVE_RULES, "Chongqing")


if __name__ == "__main__":
    asyncio.run(main())
