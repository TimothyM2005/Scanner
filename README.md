# Scanner

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install -r requirements.txt
```

## Run

```powershell
python internet_scanner.py --output fsae_matches --max-pages 500 --crawl-depth 1 --workers 16 --respect-robots --resume
```

## Optimizations included

- URL canonicalization + dedupe (removes fragment/tracking duplicates).
- DDGS pagination with optional HTML fallback pagination.
- Multi-threaded fetching with retries/backoff and per-host pacing.
- Domain limits (`--max-pages-per-domain`) for broader site diversity.
- SQLite cache (`--cache-db`, `--cache-ttl-hours`) to avoid re-downloading unchanged recent pages.
- robots.txt support (`--respect-robots`).
- PDF text extraction (`pypdf`) for PDF-only technical docs.
- Checkpoint/resume (`--checkpoint-path`, `--checkpoint-every`, `--resume`).
- Ranked output sorted by usefulness.

## Output files

- `<output>.json`
- `<output>.csv` (ordered by `score`, `group_weight_sum`, `match_term_count`)
- `<output>_normalized.csv` (`domain`, `path_depth`, `doc_type`, scores)

## Useful flags

- `--max-results-per-query 0`: no fixed per-query seed cap (paginate until stopping conditions).
- `--search-page-cap 200`: search pagination upper bound per query.
- `--max-pages`: crawl budget.
- `--max-pages-per-domain`: cap per domain (default 20).
- `--host-interval-seconds`: min spacing between requests to same host.
