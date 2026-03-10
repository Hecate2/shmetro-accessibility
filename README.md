# Shanghai Metro Accessibility Crawler

This script now uses AMap Web Service APIs to estimate directed travel time from every Shanghai metro station node to every other station node around a weekday morning departure time.

## What Changed

- Route planning uses AMap public transit planning instead of the Shanghai Metro mobile site.
- Crawling is fully asynchronous and concurrency is configurable.
- Progress is persisted in SQLite so interrupted runs can resume.
- Station nodes stay line-specific and are not merged by identical names.
  - Example: `2号线 浦东南路` and `14号线 浦东南路` are treated as different nodes.
  - Example: `1/3/4号线 上海火车站` are treated as different nodes.
- AMap station matching prefers line-specific subway exits so inconvenient transfers are reflected in walking time.
- Route selection rejects any plan containing taxi or maglev.
- Travel times are directional `A -> B`, not assumed symmetric.

## Prerequisites

- You need an AMap Web Service key.
- The AMap account should complete personal developer verification before using the crawler.

## Inputs

- `output/stations_all.csv`
  - Existing station catalog used as the node list.
- `.env`
  - You can use a single credential pair:

```bash
KEY=your_amap_key
SEC=your_amap_secret
```

  - Or multiple credential pairs for higher aggregate concurrency:

```bash
AMAP_KEY_1=your_amap_key_1
AMAP_SEC_1=your_amap_secret_1
AMAP_KEY_2=your_amap_key_2
AMAP_SEC_2=your_amap_secret_2
AMAP_KEY_3=your_amap_key_3
AMAP_SEC_3=your_amap_secret_3
```

Put the AMap Web Service key in `KEY` or `AMAP_KEY_n`, and put the digital signature secret in `SEC` or `AMAP_SEC_n`.

The script computes `sig` automatically for every AMap request.

## Run

Resolve station nodes and crawl all directed routes:

```bash
python3 shmetro_accessibility.py
```

Useful options:

```bash
python3 shmetro_accessibility.py --resolve-only
python3 shmetro_accessibility.py --compute-only
python3 shmetro_accessibility.py --route-workers 4 --resolve-workers 2
python3 shmetro_accessibility.py --date 2026-03-10 --time 7:15
python3 shmetro_accessibility.py --db-path output/amap_transit.db
```

Important flags:

- `--resolve-only` only resolves station nodes to AMap POIs or exits.
- `--compute-only` skips network calls and rebuilds outputs from the SQLite cache.
- `--route-workers` controls concurrent route requests.
- `--resolve-workers` controls concurrent station matching requests.
- `--station-search-qps` hard-caps station search requests per credential and defaults to `3.1` QPS.
- `--route-plan-qps` hard-caps route planning requests per credential and defaults to `3.1` QPS.
- `--pause` adds an optional extra delay after successful AMap calls and defaults to `0`.
- `--strategy 7` uses AMap metro-priority public transit mode by default.

## SQLite Schema

Default database: `output/amap_transit.db`

Tables:

- `station_amap`
  - Stores the matched AMap POI or exit for each line-specific station node.
- `route_times`
  - Stores directed route results from `from_id -> to_id`.
  - `status='done'` means a valid public-transit route was found.
  - `status='no_valid_route'` means AMap returned no acceptable plan after filtering taxi and maglev.
  - `status='error'` means the request failed and will be retried on the next run.

## Output Files

- `output/amap_station_matches.csv`
- `output/amap_station_matches.md`
- `output/amap_transit.db`
- `output/travel_time_matrix.csv`
- `output/travel_time_pairs.md`
- `output/average_time_ranking.csv`
- `output/average_time_ranking.md`

## Notes

- The route matrix is directional because entrances, walking time, and service patterns can differ by direction.
- Each credential pair has an independent `3.1` QPS cap for station search and route planning by default.
- When multiple credential pairs are configured, the crawler rotates across them to raise aggregate throughput while keeping each pair inside its own cap.
- AMap can still rate-limit requests, so modest concurrency is safer for long runs.
- If some station nodes remain unresolved, their routes will be left blank in the outputs until a later rerun resolves them.