#!/usr/bin/env python3
"""方向 3：零 API 成本的数据质量审计。

在已爬的 route_times 上检测三类异常，结果写入 route_flags 表 + route_audit.md：
1. triangle_slow / triangle_fast：t(A->C) 与 min_B[t(A->B)+t(B->C)] 的偏差超过阈值
   （同一 7:15 出发口径下经 B 分解是下界；偏慢尾部是错配 POI 的信号——同一原点
   集中出现 triangle_slow 时该站 POI 疑似配到了远处出口；偏快 mostly 是两段换乘
   开销 vs 直达的正常差距，只看极端值）；
2. direction_asym：|t(A->B) - t(B->A)| 过大；
3. group_offset：同名多成员组内两行到共同目的地存在稳定固定差（错配出口/POI 的信号）。

flag 按 (from_id, to_id, kind) 去重，重复运行整表刷新。只读 route_times，不修改爬取数据。
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

SLOW_MIN_MINUTES = 12.0      # t(A->C) - via_min 超过此值判为异常偏慢（P99≈10.3，阈值在尾巴上）
FAST_MIN_MINUTES = 20.0      # 直达显著快于任何两段分解多半是换乘开销，只有极端值才值得看
ASYM_MIN_DIFF_MINUTES = 12.0
ASYM_MIN_RATIO = 0.3
GROUP_OFFSET_MIN_MINUTES = 4.0
GROUP_OFFSET_MAX_IQR_MINUTES = 2.0
MIN_INTERMEDIATES = 3        # 至少几个可达中间站才下结论，避免覆盖稀疏误报

FLAGS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS route_flags(
        from_id TEXT NOT NULL,
        to_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        detail TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        PRIMARY KEY(from_id, to_id, kind)
    )
"""


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return float("nan")
    k = (len(sorted_values) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def iqr(values: List[float]) -> float:
    if len(values) < 4:
        return 0.0
    s = sorted(values)
    return percentile(s, 0.75) - percentile(s, 0.25)


def median(values: List[float]) -> float:
    s = sorted(values)
    return percentile(s, 0.5)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="output/amap_transit.db")
    parser.add_argument("--output", default="output", help="报告输出目录")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute(FLAGS_TABLE_SQL)
    station_name: Dict[str, str] = {}
    for station_id, name in conn.execute("SELECT station_id, station_name FROM station_amap"):
        station_name[station_id] = name

    out_map: Dict[str, Dict[str, float]] = defaultdict(dict)
    for from_id, to_id, status, duration in conn.execute(
        "SELECT from_id, to_id, status, duration_seconds FROM route_times WHERE status='done'"
    ):
        if from_id == to_id or from_id not in station_name or to_id not in station_name:
            continue
        if duration is None:
            continue
        out_map[from_id][to_id] = duration / 60.0
    in_map: Dict[str, Dict[str, float]] = defaultdict(dict)
    for from_id, outs in out_map.items():
        for to_id, minutes in outs.items():
            in_map[to_id][from_id] = minutes

    flags: List[Tuple[str, str, str, str]] = []
    excess_samples: List[float] = []
    asym_samples: List[float] = []

    for origin, outs in out_map.items():
        for dest, t_od in outs.items():
            # 方向不对称独立于三角形门槛：只要有反向实测就检查
            t_rev = out_map.get(dest, {}).get(origin)
            if t_rev is not None and origin < dest:
                diff = abs(t_od - t_rev)
                asym_samples.append(diff)
                lo = min(t_od, t_rev)
                if diff >= ASYM_MIN_DIFF_MINUTES and lo > 0 and diff / lo >= ASYM_MIN_RATIO:
                    flags.append(
                        (
                            origin,
                            dest,
                            "direction_asym",
                            f"t={t_od:.1f} reverse={t_rev:.1f} diff={diff:.1f}",
                        )
                    )

            ins_dest = in_map.get(dest, {})
            best_via: Optional[float] = None
            best_b = ""
            candidates = 0
            for b, t_ob in outs.items():
                if b == dest:
                    continue
                t_bd = ins_dest.get(b)
                if t_bd is None:
                    continue
                candidates += 1
                via = t_ob + t_bd
                if best_via is None or via < best_via:
                    best_via = via
                    best_b = b
            if best_via is None or candidates < MIN_INTERMEDIATES:
                continue
            excess = t_od - best_via
            excess_samples.append(excess)
            detail = f"t={t_od:.1f} via {station_name[best_b]}({best_b})={best_via:.1f} excess={excess:.1f}"
            if excess >= SLOW_MIN_MINUTES:
                flags.append((origin, dest, "triangle_slow", detail))
            elif excess <= -FAST_MIN_MINUTES:
                flags.append((origin, dest, "triangle_fast", detail))

    groups: Dict[str, List[str]] = defaultdict(list)
    for station_id, name in station_name.items():
        groups[name].append(station_id)
    group_offset_notes: List[str] = []
    for name, members in groups.items():
        if len(members) < 2:
            continue
        resolved_members = [m for m in members if m in out_map]
        if len(resolved_members) < 2:
            continue
        base = resolved_members[0]
        for other in resolved_members[1:]:
            common = set(out_map[base]) & set(out_map[other])
            if len(common) < 5:
                continue
            deltas = [out_map[other][d] - out_map[base][d] for d in common]
            med = median(deltas)
            spread = iqr(deltas)
            if abs(med) >= GROUP_OFFSET_MIN_MINUTES and spread <= GROUP_OFFSET_MAX_IQR_MINUTES:
                note = (
                    f"{name}: {other} 相对 {base} 到 {len(common)} 个共同目的地"
                    f"中位偏移 {med:+.1f}min, IQR {spread:.1f}min"
                )
                group_offset_notes.append(note)
                flags.append((base, other, "group_offset", note))

    now = datetime.now().isoformat(timespec="seconds")
    conn.execute("DELETE FROM route_flags")
    conn.executemany(
        "INSERT OR REPLACE INTO route_flags(from_id, to_id, kind, detail, created_at) VALUES(?,?,?,?,?)",
        [(f, t, k, d, now) for f, t, k, d in flags],
    )
    conn.commit()

    excess_sorted = sorted(excess_samples)
    asym_sorted = sorted(asym_samples)
    kind_counts: Dict[str, int] = defaultdict(int)
    origin_counts: Dict[str, int] = defaultdict(int)
    for f, _, kind, _ in flags:
        kind_counts[kind] += 1
        origin_counts[f] += 1

    lines = [
        "# Route Data Quality Audit",
        "",
        f"- audited at: {now}",
        f"- done pairs: {sum(len(v) for v in out_map.values())}",
        f"- pairs with >= {MIN_INTERMEDIATES} intermediate hubs: {len(excess_samples)}",
        "",
        "## excess = t(A->C) - min_B[t(A->B)+t(B->C)] minutes",
        f"- P50={percentile(excess_sorted, 0.5):.1f} P90={percentile(excess_sorted, 0.9):.1f} "
        f"P99={percentile(excess_sorted, 0.99):.1f} min={excess_sorted[0]:.1f} max={excess_sorted[-1]:.1f}"
        if excess_sorted
        else "- no samples",
        "",
        "## |t(A->B) - t(B->A)| minutes",
        f"- P50={percentile(asym_sorted, 0.5):.1f} P90={percentile(asym_sorted, 0.9):.1f} "
        f"P99={percentile(asym_sorted, 0.99):.1f} max={asym_sorted[-1]:.1f}"
        if asym_sorted
        else "- no samples",
        "",
        "## flags",
    ]
    for kind in sorted(kind_counts):
        lines.append(f"- {kind}: {kind_counts[kind]}")
    lines += ["", "## top flagged origins（集中在同一原点 => 该站 POI 疑似错配）"]
    for origin, count in sorted(origin_counts.items(), key=lambda kv: -kv[1])[:15]:
        lines.append(f"- {station_name.get(origin, '?')} ({origin}): {count} flags")
    if group_offset_notes:
        lines += ["", "## group_offset notes"]
        lines += [f"- {n}" for n in group_offset_notes]
    report = "\n".join(lines) + "\n"

    from pathlib import Path

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "route_audit.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
