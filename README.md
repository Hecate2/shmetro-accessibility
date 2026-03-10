# Shanghai Metro Accessibility Crawler

This script uses asynchronous HTTP requests to fetch station data from Shanghai Metro mobile endpoints, computes pairwise travel-time costs, stores results in a WAL‑mode SQLite database, and produces human-readable and CSV outputs including an average travel-time ranking.

## What it does

1. Fetches all stations for lines:
   - `1..18`
   - `41` (浦江线)
   - `51` (市域机场线)
2. Writes or loads human-readable station lists cached in CSV/Markdown.
3. Queries travel time between station pairs using:
   - `func=plantrip`
   - `impedancevalue` as travel-time minutes.
4. Uses symmetry optimization:
   - queries only `A -> B` for `A < B`
   - fills `B -> A` with same value.
5. Treats empty `pathList` as same physical station and assigns `0`.
6. Computes average travel time per station to all **other non-equivalent** stations.
7. If `output/stations_all.csv` and `output/stations_by_line.md` already exist, station crawling is skipped and station data is loaded from disk.
8. Shows pair-crawling progress with ETA using `tqdm`. The crawler retries failed requests and can recover from partial data stored in the database.

## Run

```bash
python3 shmetro_accessibility.py --output output        # default crawling mode
python3 shmetro_accessibility.py --output output --compute-only  # use existing cache, skip network
```

Optional flags:

- `--workers 16` number of parallel tasks used when collecting pairwise times (default 16).
- `--pause 0.15` pause seconds between requests.
- `--timeout 15` HTTP timeout.
- `--retries 3` request retry count.
- `--compute-only` skip network crawling and regenerate outputs from cache (requires existing station files and database).

Progress display:

- Pair crawling uses a `tqdm` progress bar with elapsed time, speed, and ETA.

The crawler will resume from an existing `time_map_checkpoint.json` if rerun; you can interrupt with Ctrl‑C and restart without losing work.

## Output files

- `output/stations_by_line.md` human-readable station list grouped by line.
- `output/stations_all.csv` full station table.
- `output/time_map_checkpoint.json` legacy JSON checkpoint (rows are still saved for debugging).
- `output/time_map.db` SQLite database storing every fetched pair and enabling resume.
- `output/travel_time_matrix.csv` full from/to matrix.
- `output/travel_time_pairs.md` human-readable pair table (upper triangle only).
- `output/average_time_ranking.csv` ranking by average minutes.
- `output/average_time_ranking.md` human-readable ranking.

## Notes

- Full execution may take a while because the number of station pairs is large.
- If API behavior changes, parsing may need adjustment.
