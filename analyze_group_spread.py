#!/usr/bin/env python3
"""量化"组代表元爬取"的精度代价，口径与实现完全一致：

- 同名成员按已解析 POI 位置聚类（<=300m 同簇，引用实现里的 _cluster_group_members）；
- 每簇代表元 = (line_order, station_id) 排序后的首个可规划成员；
- 对任一成员对 done 的簇对，生产误差 = t(代表->代表) - min(全部已爬成员对)：
  - 全体口径：所有代表对已爬的簇对（单观测簇对误差恒 0，属生产真实值）；
  - 多观测子集：>=2 个成员观测的簇对（唯一能看见"尾巴"的诚实样本）。
另保留组内 max-min spread 与收尾成本对比。只读，不修改爬取数据。
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from typing import Dict, List, Tuple

from metro_accessibility_common import (
    ResolvedStation,
    Station,
    _cluster_group_members,
    resolved_station_can_plan_route,
)


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return float("nan")
    k = (len(sorted_values) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def report(name: str, values: List[float]) -> None:
    if not values:
        print(f"{name}: 无样本")
        return
    s = sorted(values)
    shares = ", ".join(
        f"≤{w}min {sum(1 for v in s if v <= w) / len(s) * 100:.2f}%" for w in (0.5, 1, 2, 3, 5)
    )
    print(
        f"{name}: n={len(s)} P50={percentile(s, 0.5):.2f} P90={percentile(s, 0.9):.2f} "
        f"P99={percentile(s, 0.99):.2f} max={s[-1]:.2f} | {shares}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="output/amap_transit.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    stations: Dict[str, Station] = {}
    resolved: Dict[str, ResolvedStation] = {}
    for row in conn.execute(
        "SELECT station_id, station_slug, station_name, line_order, line_label, source_key, "
        "query_text, poi_id, poi_name, poi_type, poi_address, location, status, score, note "
        "FROM station_amap"
    ):
        station = Station(*row[:6])
        stations[station.station_id] = station
        resolved[station.station_id] = ResolvedStation(*row)

    done: Dict[Tuple[str, str], float] = {}
    for from_id, to_id, status, duration in conn.execute(
        "SELECT from_id, to_id, status, duration_seconds FROM route_times WHERE status='done'"
    ):
        if duration is None or from_id == to_id:
            continue
        done[(from_id, to_id)] = duration / 60.0

    # 与实现一致：同名组 -> POI 距离聚类 -> 每簇一个代表元
    name_groups: Dict[str, List[Station]] = defaultdict(list)
    for station in stations.values():
        name_groups[station.station_name].append(station)
    clusters: List[List[Station]] = []
    for members in name_groups.values():
        clusters.extend(_cluster_group_members(members, resolved))
    cluster_of: Dict[str, int] = {}
    for index, members in enumerate(clusters):
        for member in members:
            cluster_of[member.station_id] = index

    def pick_rep(members: List[Station]) -> str | None:
        ordered = sorted(members, key=lambda s: (s.line_order, s.station_id))
        for member in ordered:
            if resolved_station_can_plan_route(resolved.get(member.station_id)):
                return member.station_id
        return None

    rep_of_cluster: Dict[int, str] = {}
    for index, members in enumerate(clusters):
        rep = pick_rep(members)
        if rep is not None:
            rep_of_cluster[index] = rep

    # 簇对 -> 已爬成员对分钟
    pair_obs: Dict[Tuple[int, int], List[float]] = defaultdict(list)
    for (from_id, to_id), minutes in done.items():
        if from_id not in cluster_of or to_id not in cluster_of:
            continue
        co, cd = cluster_of[from_id], cluster_of[to_id]
        if co == cd:
            continue
        pair_obs[(co, cd)].append(minutes)

    prod_err: List[float] = []       # 全体口径：代表对已爬的簇对
    multi_err: List[float] = []      # 多观测子集（诚实尾巴）
    multi_spread: List[float] = []   # 多观测簇对的 max-min
    tail_groups: set = set()
    for (co, cd), obs in pair_obs.items():
        ro, rd = rep_of_cluster.get(co), rep_of_cluster.get(cd)
        if ro is None or rd is None:
            continue
        rep_minutes = done.get((ro, rd))
        if rep_minutes is None:
            continue
        best = min(obs)
        err = rep_minutes - best
        prod_err.append(max(0.0, err))
        if len(obs) >= 2:
            multi_err.append(max(0.0, err))
            multi_spread.append(max(obs) - best)
            if err > 3.0:
                tail_groups.add(stations[ro].station_name)

    print("== 生产口径：簇代表 rep->rep 误差（t(rep,rep) - 簇内已爬最优，分钟）==")
    report("全体（含单观测簇对，误差恒 0）", prod_err)
    report("多观测子集（诚实尾巴）", multi_err)
    print()
    print("== 多观测簇对组内 spread（max - min，分钟）==")
    report("全部多观测簇对", multi_spread)
    if tail_groups:
        print(f"误差>3min 涉及 {len(tail_groups)} 个起点簇: {sorted(tail_groups)}")

    n_nodes = len(stations)
    n_clusters = len(clusters)
    total_node_pairs = n_nodes * (n_nodes - 1)
    total_cluster_pairs = n_clusters * (n_clusters - 1)
    print()
    print("== 收尾成本对比 ==")
    print(f"节点级全量: {total_node_pairs} 对")
    print(f"簇级全量:   {total_cluster_pairs} 对 (省 {100 * (1 - total_cluster_pairs / total_node_pairs):.1f}%)")
    print(f"已覆盖簇对(任一成员done): {len(pair_obs)}")
    print(f"剩余新增: 节点级 {total_node_pairs - len(done)} vs 簇级 {total_cluster_pairs - len(pair_obs)}")


if __name__ == "__main__":
    main()
