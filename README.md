WebScanner

## What it does

1. Runs multiple search queries to discover candidate URLs.
2. Downloads those pages and optionally follows links to a small crawl depth.
3. Scores each page based on keyword groups from `scanner_keywords.yaml`.
4. Saves matched pages to:
   - `fsae_matches.json`
   - `fsae_matches.csv`

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

If your system has no `py` launcher, always use `python` as shown above.

## Add terms to the `scanner_keywords.yaml` File. Add the current contents of the file into chat and give the thing that you are looking for then have it generate a list of perameters and weights

## Run

```powershell
python internet_scanner.py --output fsae_matches --max-pages 300 --crawl-depth 1 --min-score 4
```

## Tune search quality

- Edit `scanner_keywords.yaml` to add team names, synonyms, or extra steering terms.
- Raise `--min-score` to reduce noise.
- Increase `--max-pages` for wider coverage.
- Increase `--crawl-depth` carefully (more pages, slower run).

## Important limits

- Scanning "all of the internet" is not feasible from one script.
- This scanner uses search-engine discovery + focused crawling.
- Some sites block bots; results vary by location and time.
- Respect robots.txt and website terms before large-scale crawling.
