#!/usr/bin/env python3
"""
FSAE web scanner:
- Finds candidate pages using DuckDuckGo text search
- Crawls discovered pages to a limited depth
- Scores and records pages that match configured keywords
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
import yaml
from bs4 import BeautifulSoup

try:
    from ddgs import DDGS  # New official package name.
except ImportError:
    from duckduckgo_search import DDGS  # Backward compatibility.

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 "
    "FSAE-Research-Scanner/1.0"
)


@dataclass
class MatchResult:
    url: str
    title: str
    score: int
    matched_groups: List[str]
    matched_terms: Dict[str, List[str]]
    snippet: str


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def extract_visible_text(html: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()

    title = soup.title.get_text(strip=True) if soup.title else ""
    body_text = soup.get_text(" ", strip=True)
    return title, body_text


def find_matches(text: str, groups: dict) -> Tuple[int, Dict[str, List[str]]]:
    matched: Dict[str, List[str]] = {}
    score = 0
    lowered = normalize_text(text)

    for group_name, group_data in groups.items():
        weight = int(group_data.get("weight", 1))
        terms = group_data.get("terms", [])
        found_terms = []
        for term in terms:
            pattern = r"\b" + re.escape(term.lower()) + r"\b"
            if re.search(pattern, lowered):
                found_terms.append(term)
        if found_terms:
            matched[group_name] = sorted(set(found_terms))
            score += weight * len(set(found_terms))

    return score, matched


def build_snippet(text: str, terms: List[str], max_chars: int = 260) -> str:
    if not text:
        return ""

    lowered = text.lower()
    positions = []
    for term in terms:
        idx = lowered.find(term.lower())
        if idx != -1:
            positions.append(idx)
    if not positions:
        return text[:max_chars]

    start = max(min(positions) - 80, 0)
    end = min(start + max_chars, len(text))
    return text[start:end].replace("\n", " ").strip()


def fetch_url(url: str, timeout: int = 15) -> Tuple[str, str, List[str]]:
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    content_type = resp.headers.get("content-type", "").lower()
    if "text/html" not in content_type:
        return "", "", []

    title, text = extract_visible_text(resp.text)
    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        candidate = urljoin(url, a["href"])
        parsed = urlparse(candidate)
        if parsed.scheme in {"http", "https"}:
            links.append(candidate)
    return title, text, links


def discover_urls(search_queries: List[str], max_results_per_query: int) -> List[str]:
    discovered = []
    seen = set()

    # Primary source: DDGS package.
    with DDGS() as ddgs:
        for q in search_queries:
            try:
                rows = ddgs.text(q, max_results=max_results_per_query)
            except Exception:
                rows = []

            for row in rows:
                url = row.get("href")
                if url and url not in seen:
                    seen.add(url)
                    discovered.append(url)

    # Fallback source: direct DuckDuckGo HTML results, if DDGS yields nothing.
    if not discovered:
        headers = {"User-Agent": USER_AGENT}
        for q in search_queries:
            try:
                resp = requests.get(
                    "https://duckduckgo.com/html/",
                    params={"q": q},
                    headers=headers,
                    timeout=20,
                )
                resp.raise_for_status()
            except requests.RequestException:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            anchors = soup.select("a.result__a")
            for a in anchors[:max_results_per_query]:
                href = (a.get("href") or "").strip()
                if href.startswith("http") and href not in seen:
                    seen.add(href)
                    discovered.append(href)

    return discovered


def scan(
    seed_urls: List[str],
    groups: dict,
    min_score: int,
    max_pages: int,
    crawl_depth: int,
    delay_seconds: float,
) -> List[MatchResult]:
    queue: List[Tuple[str, int]] = [(u, 0) for u in seed_urls]
    visited: Set[str] = set()
    results: List[MatchResult] = []

    while queue and len(visited) < max_pages:
        url, depth = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        try:
            title, text, links = fetch_url(url)
            if not text:
                continue

            score, matched = find_matches(f"{title} {text}", groups)
            if score >= min_score:
                flat_terms = [t for terms in matched.values() for t in terms]
                snippet = build_snippet(text, flat_terms)
                results.append(
                    MatchResult(
                        url=url,
                        title=title[:240],
                        score=score,
                        matched_groups=sorted(matched.keys()),
                        matched_terms=matched,
                        snippet=snippet,
                    )
                )

            if depth < crawl_depth:
                for link in links[:100]:
                    if link not in visited:
                        queue.append((link, depth + 1))

        except requests.RequestException:
            pass
        finally:
            time.sleep(delay_seconds)

    results.sort(key=lambda r: r.score, reverse=True)
    return results


def save_results(results: List[MatchResult], output_base: Path) -> None:
    json_path = output_base.with_suffix(".json")
    csv_path = output_base.with_suffix(".csv")

    with json_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2, ensure_ascii=False)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["url", "title", "score", "matched_groups", "matched_terms", "snippet"],
        )
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "url": r.url,
                    "title": r.title,
                    "score": r.score,
                    "matched_groups": ", ".join(r.matched_groups),
                    "matched_terms": json.dumps(r.matched_terms, ensure_ascii=False),
                    "snippet": r.snippet,
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan web pages for FSAE steering content.")
    parser.add_argument("--config", default="scanner_keywords.yaml", help="Path to YAML config.")
    parser.add_argument("--output", default="fsae_matches", help="Output file base name.")
    parser.add_argument("--max-results-per-query", type=int, default=30)
    parser.add_argument("--max-pages", type=int, default=250)
    parser.add_argument("--crawl-depth", type=int, default=1)
    parser.add_argument("--min-score", type=int, default=4)
    parser.add_argument("--delay-seconds", type=float, default=0.7)
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    queries = cfg.get("search_queries", [])
    groups = cfg.get("keyword_groups", {})
    if not queries or not groups:
        raise ValueError("Config requires search_queries and keyword_groups.")

    print("Discovering URLs from search queries...")
    seed_urls = discover_urls(queries, args.max_results_per_query)
    print(f"Discovered {len(seed_urls)} seed URLs.")

    print("Scanning pages...")
    results = scan(
        seed_urls=seed_urls,
        groups=groups,
        min_score=args.min_score,
        max_pages=args.max_pages,
        crawl_depth=args.crawl_depth,
        delay_seconds=args.delay_seconds,
    )
    save_results(results, Path(args.output))
    print(f"Saved {len(results)} matches to {args.output}.json and {args.output}.csv")


if __name__ == "__main__":
    main()
