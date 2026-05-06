#!/usr/bin/env python3
"""
FSAE web scanner (optimized):
- Search discovery via DDGS (with pagination) + HTML fallback
- Canonical URL dedupe
- Parallel fetching with retries/backoff and per-host cooldown
- Optional robots.txt checks
- SQLite page cache (HTML and PDF extracted text)
- Resume/checkpoint support for interrupted runs
- Domain-level crawl limits and domain-diverse prioritization
- Ranked CSV/JSON outputs + normalized analysis CSV
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import requests
import yaml
from bs4 import BeautifulSoup
from pypdf import PdfReader

try:
    from ddgs import DDGS
except ImportError:
    from duckduckgo_search import DDGS

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 "
    "FSAE-Research-Scanner/2.0"
)

STRIP_QUERY_KEYS = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "fbclid",
        "gclid",
        "mc_cid",
        "mc_eid",
    }
)


@dataclass
class MatchResult:
    url: str
    title: str
    score: int
    matched_groups: List[str]
    matched_terms: Dict[str, List[str]]
    snippet: str
    domain: str
    doc_type: str


@dataclass
class FetchResult:
    url: str
    title: str
    text: str
    links: List[str]
    doc_type: str


def canonicalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}:
        return ""
    netloc = parsed.netloc.lower()
    if netloc.endswith(":80") and parsed.scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and parsed.scheme == "https":
        netloc = netloc[:-4]
    path = parsed.path or "/"
    pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in STRIP_QUERY_KEYS]
    query = urlencode(pairs, doseq=True)
    return urlunparse((parsed.scheme, netloc, path, "", query, ""))


def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()


def path_depth(url: str) -> int:
    p = urlparse(url).path.strip("/")
    return 0 if not p else len([part for part in p.split("/") if part])


def dedupe_urls_preserve_order(urls: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for url in urls:
        canon = canonicalize_url(url)
        if canon and canon not in seen:
            seen.add(canon)
            out.append(canon)
    return out


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def extract_visible_text(html: str) -> Tuple[str, str, List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    title = soup.title.get_text(strip=True) if soup.title else ""
    body = soup.get_text(" ", strip=True)
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        c = canonicalize_url(a["href"])
        if not c:
            links.append(a["href"])
        else:
            links.append(c)
    return title, body, links


def find_matches(text: str, groups: dict) -> Tuple[int, Dict[str, List[str]]]:
    lowered = normalize_text(text)
    score = 0
    matched: Dict[str, List[str]] = {}
    for name, data in groups.items():
        weight = int(data.get("weight", 1))
        found: Set[str] = set()
        for term in data.get("terms", []):
            if re.search(r"\b" + re.escape(term.lower()) + r"\b", lowered):
                found.add(term)
        if found:
            matched[name] = sorted(found)
            score += weight * len(found)
    return score, matched


def build_snippet(text: str, terms: List[str], max_chars: int = 260) -> str:
    if not text:
        return ""
    lowered = text.lower()
    positions = [lowered.find(t.lower()) for t in terms if lowered.find(t.lower()) != -1]
    if not positions:
        return text[:max_chars]
    start = max(min(positions) - 80, 0)
    end = min(start + max_chars, len(text))
    return text[start:end].replace("\n", " ").strip()


def hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


class CacheDB:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                fetched_at REAL NOT NULL,
                content_hash TEXT NOT NULL,
                title TEXT NOT NULL,
                text TEXT NOT NULL,
                links_json TEXT NOT NULL,
                doc_type TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def get(self, url: str, ttl_hours: float) -> Optional[FetchResult]:
        cutoff = time.time() - (ttl_hours * 3600.0)
        with self._lock:
            row = self._conn.execute(
                "SELECT fetched_at, title, text, links_json, doc_type FROM pages WHERE url=?",
                (url,),
            ).fetchone()
        if not row:
            return None
        fetched_at, title, text, links_json, doc_type = row
        if fetched_at < cutoff:
            return None
        try:
            links = json.loads(links_json)
        except Exception:
            links = []
        return FetchResult(url=url, title=title, text=text, links=links, doc_type=doc_type)

    def upsert(self, result: FetchResult) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO pages(url, fetched_at, content_hash, title, text, links_json, doc_type)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(url) DO UPDATE SET
                    fetched_at=excluded.fetched_at,
                    content_hash=excluded.content_hash,
                    title=excluded.title,
                    text=excluded.text,
                    links_json=excluded.links_json,
                    doc_type=excluded.doc_type
                """,
                (
                    result.url,
                    time.time(),
                    hash_content(result.text),
                    result.title,
                    result.text,
                    json.dumps(result.links, ensure_ascii=False),
                    result.doc_type,
                ),
            )
            self._conn.commit()


class RobotsGuard:
    def __init__(self, enabled: bool):
        self.enabled = enabled
        self.parsers: Dict[str, RobotFileParser] = {}
        self.lock = threading.Lock()

    def allows(self, url: str) -> bool:
        if not self.enabled:
            return True
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if not domain:
            return False
        with self.lock:
            if domain not in self.parsers:
                rp = RobotFileParser()
                rp.set_url(f"{parsed.scheme}://{domain}/robots.txt")
                try:
                    rp.read()
                except Exception:
                    pass
                self.parsers[domain] = rp
            rp = self.parsers[domain]
        try:
            return rp.can_fetch(USER_AGENT, url)
        except Exception:
            return True


class HostRateLimiter:
    def __init__(self, min_interval_seconds: float):
        self.min_interval = max(0.0, min_interval_seconds)
        self.lock = threading.Lock()
        self.next_allowed: Dict[str, float] = defaultdict(float)

    def wait_turn(self, url: str) -> None:
        if self.min_interval <= 0:
            return
        host = domain_of(url)
        while True:
            with self.lock:
                now = time.time()
                allow_at = self.next_allowed.get(host, now)
                if now >= allow_at:
                    self.next_allowed[host] = now + self.min_interval
                    return
                sleep_for = allow_at - now
            time.sleep(min(0.5, sleep_for))


def _ddgs_text_page(ddgs: Any, query: str, max_results: int, page: int) -> List[Dict[str, Any]]:
    try:
        return list(ddgs.text(query, max_results=max_results, page=page))
    except TypeError:
        if page > 1:
            return []
        return list(ddgs.text(query, max_results=max_results))


def _discover_urls_ddgs(
    ddgs: Any,
    search_queries: List[str],
    max_results_per_query: int,
    search_page_cap: int,
    delay_between_pages: float,
) -> List[str]:
    discovered: List[str] = []
    seen: Set[str] = set()
    per_page = 50
    for query in search_queries:
        count = 0
        no_new = 0
        page = 1
        while page <= max(1, search_page_cap):
            chunk = per_page if max_results_per_query == 0 else min(per_page, max_results_per_query - count)
            if chunk <= 0:
                break
            try:
                rows = _ddgs_text_page(ddgs, query, chunk, page)
            except Exception:
                rows = []
            if not rows:
                break
            new_count = 0
            for row in rows:
                href = canonicalize_url(row.get("href", ""))
                if not href or href in seen:
                    continue
                seen.add(href)
                discovered.append(href)
                count += 1
                new_count += 1
                if max_results_per_query > 0 and count >= max_results_per_query:
                    break
            if max_results_per_query > 0 and count >= max_results_per_query:
                break
            if new_count == 0:
                no_new += 1
                if no_new >= 2:
                    break
            else:
                no_new = 0
            page += 1
            if delay_between_pages > 0:
                time.sleep(delay_between_pages)
    return discovered


def _discover_urls_ddg_html(
    search_queries: List[str],
    max_results_per_query: int,
    search_page_cap: int,
    delay_between_pages: float,
) -> List[str]:
    discovered: List[str] = []
    seen: Set[str] = set()
    headers = {"User-Agent": USER_AGENT}
    for query in search_queries:
        count = 0
        for page_idx in range(max(1, search_page_cap)):
            if max_results_per_query > 0 and count >= max_results_per_query:
                break
            try:
                resp = requests.get(
                    "https://duckduckgo.com/html/",
                    params={"q": query, "s": page_idx * 30},
                    headers=headers,
                    timeout=20,
                )
                resp.raise_for_status()
            except requests.RequestException:
                break
            soup = BeautifulSoup(resp.text, "html.parser")
            anchors = soup.select("a.result__a")
            if not anchors:
                break
            new_count = 0
            for a in anchors:
                href = canonicalize_url((a.get("href") or "").strip())
                if not href or href in seen:
                    continue
                seen.add(href)
                discovered.append(href)
                count += 1
                new_count += 1
                if max_results_per_query > 0 and count >= max_results_per_query:
                    break
            if new_count == 0:
                break
            if delay_between_pages > 0:
                time.sleep(delay_between_pages)
    return discovered


def discover_urls(
    search_queries: List[str],
    max_results_per_query: int,
    search_page_cap: int,
    delay_between_pages: float,
) -> List[str]:
    with DDGS() as ddgs:
        urls = _discover_urls_ddgs(ddgs, search_queries, max_results_per_query, search_page_cap, delay_between_pages)
    if urls:
        return urls
    return _discover_urls_ddg_html(search_queries, max_results_per_query, search_page_cap, delay_between_pages)


def extract_pdf_text(content: bytes, max_pages: int) -> str:
    try:
        reader = PdfReader(io.BytesIO(content))
    except Exception:
        return ""
    chunks: List[str] = []
    for i, page in enumerate(reader.pages):
        if i >= max_pages:
            break
        try:
            chunks.append(page.extract_text() or "")
        except Exception:
            continue
    return " ".join(chunks).strip()


def fetch_from_web(
    session: requests.Session,
    url: str,
    timeout: int,
    retries: int,
    backoff_base: float,
    robots: RobotsGuard,
    host_limiter: HostRateLimiter,
    pdf_max_pages: int,
) -> Optional[FetchResult]:
    if not robots.allows(url):
        return None
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(retries + 1):
        host_limiter.wait_turn(url)
        try:
            resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if resp.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"retryable status {resp.status_code}")
            resp.raise_for_status()
        except Exception:
            if attempt >= retries:
                return None
            sleep_s = backoff_base * (2 ** attempt)
            time.sleep(min(15.0, sleep_s))
            continue

        final_url = canonicalize_url(resp.url)
        ctype = (resp.headers.get("content-type") or "").lower()
        if "application/pdf" in ctype or final_url.endswith(".pdf"):
            text = extract_pdf_text(resp.content, pdf_max_pages)
            if not text:
                return None
            return FetchResult(url=final_url, title="PDF Document", text=text, links=[], doc_type="pdf")
        if "text/html" not in ctype:
            return None
        title, text, links_raw = extract_visible_text(resp.text)
        links: List[str] = []
        for href in links_raw:
            link = canonicalize_url(urljoin(final_url, href))
            if link:
                links.append(link)
        return FetchResult(url=final_url, title=title[:240], text=text, links=links, doc_type="html")
    return None


def _merge_match_results(a: MatchResult, b: MatchResult) -> MatchResult:
    first, second = (a, b) if a.score >= b.score else (b, a)
    merged: Dict[str, Set[str]] = {}
    for name, terms in first.matched_terms.items():
        merged[name] = set(terms)
    for name, terms in second.matched_terms.items():
        merged.setdefault(name, set()).update(terms)
    out = {k: sorted(v) for k, v in sorted(merged.items())}
    return MatchResult(
        url=first.url,
        title=first.title or second.title,
        score=max(first.score, second.score),
        matched_groups=sorted(out.keys()),
        matched_terms=out,
        snippet=first.snippet or second.snippet,
        domain=domain_of(first.url),
        doc_type=first.doc_type,
    )


def _result_from_fetch(fetch: FetchResult, groups: dict, min_score: int) -> Optional[MatchResult]:
    score, matched = find_matches(f"{fetch.title} {fetch.text}", groups)
    if score < min_score:
        return None
    flat_terms = [t for terms in matched.values() for t in terms]
    return MatchResult(
        url=fetch.url,
        title=fetch.title,
        score=score,
        matched_groups=sorted(matched.keys()),
        matched_terms=matched,
        snippet=build_snippet(fetch.text, flat_terms),
        domain=domain_of(fetch.url),
        doc_type=fetch.doc_type,
    )


def _group_weight_sum(match: MatchResult, groups: dict) -> int:
    return sum(int(groups.get(name, {}).get("weight", 1)) for name in match.matched_terms)


def _match_term_count(match: MatchResult) -> int:
    return sum(len(terms) for terms in match.matched_terms.values())


def sort_results_by_relevance(results: List[MatchResult], groups: dict) -> List[MatchResult]:
    return sorted(
        results,
        key=lambda r: (-r.score, -_group_weight_sum(r, groups), -_match_term_count(r), r.url),
    )


def save_checkpoint(
    checkpoint_path: Path,
    depth: int,
    links_from_depth: List[str],
    visited: Set[str],
    domain_counts: Dict[str, int],
    results_by_url: Dict[str, MatchResult],
    fetch_count: int,
) -> None:
    payload = {
        "depth": depth,
        "links_from_depth": links_from_depth,
        "visited": sorted(visited),
        "domain_counts": domain_counts,
        "results": [asdict(v) for v in results_by_url.values()],
        "fetch_count": fetch_count,
    }
    checkpoint_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def load_checkpoint(checkpoint_path: Path) -> Optional[dict]:
    if not checkpoint_path.exists():
        return None
    try:
        return json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def prioritize_batch(batch: List[str], domain_counts: Dict[str, int], max_pages_per_domain: int) -> List[str]:
    filtered = [u for u in batch if domain_counts.get(domain_of(u), 0) < max_pages_per_domain]
    return sorted(filtered, key=lambda u: (domain_counts.get(domain_of(u), 0), domain_of(u), u))


def scan(
    seed_urls: List[str],
    groups: dict,
    min_score: int,
    max_pages: int,
    crawl_depth: int,
    workers: int,
    fetch_timeout: int,
    max_pages_per_domain: int,
    cache_db: CacheDB,
    cache_ttl_hours: float,
    retries: int,
    backoff_base: float,
    host_interval_seconds: float,
    robots_enabled: bool,
    pdf_max_pages: int,
    checkpoint_path: Path,
    checkpoint_every: int,
    resume: bool,
) -> List[MatchResult]:
    seeds = dedupe_urls_preserve_order(seed_urls)
    visited: Set[str] = set()
    domain_counts: Dict[str, int] = defaultdict(int)
    results_by_url: Dict[str, MatchResult] = {}
    links_from_depth: List[str] = []
    fetch_count = 0
    start_depth = 0

    if resume:
        saved = load_checkpoint(checkpoint_path)
        if saved:
            visited = set(saved.get("visited", []))
            links_from_depth = dedupe_urls_preserve_order(saved.get("links_from_depth", []))
            domain_counts.update(saved.get("domain_counts", {}))
            fetch_count = int(saved.get("fetch_count", 0))
            start_depth = int(saved.get("depth", 0))
            for item in saved.get("results", []):
                mr = MatchResult(**item)
                results_by_url[mr.url] = mr

    robots = RobotsGuard(enabled=robots_enabled)
    limiter = HostRateLimiter(min_interval_seconds=host_interval_seconds)
    session = requests.Session()

    for depth in range(start_depth, crawl_depth + 1):
        if depth == 0 and not links_from_depth:
            batch = [u for u in seeds if u not in visited]
        else:
            batch = [u for u in dedupe_urls_preserve_order(links_from_depth) if u not in visited]
            links_from_depth = []
        batch = prioritize_batch(batch, domain_counts, max_pages_per_domain)
        remaining = max_pages - fetch_count
        if remaining <= 0 or not batch:
            break
        batch = batch[:remaining]
        for u in batch:
            visited.add(u)
            domain_counts[domain_of(u)] += 1
        fetch_count += len(batch)

        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {}
            for url in batch:
                cached = cache_db.get(url, ttl_hours=cache_ttl_hours)
                if cached:
                    futures[executor.submit(lambda x: x, cached)] = True
                else:
                    fut = executor.submit(
                        fetch_from_web,
                        session,
                        url,
                        fetch_timeout,
                        retries,
                        backoff_base,
                        robots,
                        limiter,
                        pdf_max_pages,
                    )
                    futures[fut] = False

            processed = 0
            for fut in as_completed(futures):
                from_cache = futures[fut]
                fetched = fut.result()
                if not fetched:
                    continue
                if not from_cache:
                    cache_db.upsert(fetched)
                match = _result_from_fetch(fetched, groups, min_score)
                if match:
                    if match.url in results_by_url:
                        results_by_url[match.url] = _merge_match_results(results_by_url[match.url], match)
                    else:
                        results_by_url[match.url] = match
                if depth < crawl_depth:
                    for link in fetched.links[:120]:
                        c = canonicalize_url(link)
                        if c and c not in visited:
                            links_from_depth.append(c)
                processed += 1
                if checkpoint_every > 0 and (processed % checkpoint_every == 0):
                    save_checkpoint(
                        checkpoint_path,
                        depth,
                        links_from_depth,
                        visited,
                        dict(domain_counts),
                        results_by_url,
                        fetch_count,
                    )

        save_checkpoint(
            checkpoint_path,
            depth + 1,
            links_from_depth,
            visited,
            dict(domain_counts),
            results_by_url,
            fetch_count,
        )

    return list(results_by_url.values())


def save_results(results: List[MatchResult], output_base: Path, groups: dict) -> None:
    ordered = sort_results_by_relevance(results, groups)
    json_path = output_base.with_suffix(".json")
    csv_path = output_base.with_suffix(".csv")
    normalized_csv_path = output_base.with_name(output_base.name + "_normalized").with_suffix(".csv")

    with json_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in ordered], f, indent=2, ensure_ascii=False)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "score",
                "group_weight_sum",
                "match_term_count",
                "domain",
                "doc_type",
                "url",
                "title",
                "matched_groups",
                "matched_terms",
                "snippet",
            ],
        )
        writer.writeheader()
        for r in ordered:
            writer.writerow(
                {
                    "score": r.score,
                    "group_weight_sum": _group_weight_sum(r, groups),
                    "match_term_count": _match_term_count(r),
                    "domain": r.domain,
                    "doc_type": r.doc_type,
                    "url": r.url,
                    "title": r.title,
                    "matched_groups": ", ".join(r.matched_groups),
                    "matched_terms": json.dumps(r.matched_terms, ensure_ascii=False),
                    "snippet": r.snippet,
                }
            )

    with normalized_csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "url",
                "domain",
                "path_depth",
                "doc_type",
                "score",
                "group_weight_sum",
                "match_term_count",
            ],
        )
        writer.writeheader()
        for r in ordered:
            writer.writerow(
                {
                    "url": r.url,
                    "domain": r.domain,
                    "path_depth": path_depth(r.url),
                    "doc_type": r.doc_type,
                    "score": r.score,
                    "group_weight_sum": _group_weight_sum(r, groups),
                    "match_term_count": _match_term_count(r),
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan web pages for FSAE steering content.")
    parser.add_argument("--config", default="scanner_keywords.yaml")
    parser.add_argument("--output", default="fsae_matches")
    parser.add_argument("--max-results-per-query", type=int, default=0)
    parser.add_argument("--search-page-cap", type=int, default=100)
    parser.add_argument("--search-page-delay", type=float, default=0.25)
    parser.add_argument("--max-pages", type=int, default=300)
    parser.add_argument("--crawl-depth", type=int, default=1)
    parser.add_argument("--min-score", type=int, default=4)
    parser.add_argument("--workers", type=int, default=max(8, (os.cpu_count() or 4) * 2))
    parser.add_argument("--fetch-timeout", type=int, default=20)
    parser.add_argument("--max-pages-per-domain", type=int, default=20)
    parser.add_argument("--cache-db", default="scanner_cache.sqlite3")
    parser.add_argument("--cache-ttl-hours", type=float, default=168.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--retry-backoff-base", type=float, default=1.2)
    parser.add_argument("--host-interval-seconds", type=float, default=0.25)
    parser.add_argument("--respect-robots", action="store_true")
    parser.add_argument("--pdf-max-pages", type=int, default=25)
    parser.add_argument("--checkpoint-path", default="scanner_checkpoint.json")
    parser.add_argument("--checkpoint-every", type=int, default=40)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    queries = cfg.get("search_queries", [])
    groups = cfg.get("keyword_groups", {})
    if not queries or not groups:
        raise ValueError("Config requires search_queries and keyword_groups.")

    print("Discovering URLs from search queries...")
    seed_urls = discover_urls(
        queries,
        args.max_results_per_query,
        args.search_page_cap,
        args.search_page_delay,
    )
    print(f"Discovered {len(seed_urls)} seed URLs.")

    cache_db = CacheDB(Path(args.cache_db))

    print("Scanning pages...")
    results = scan(
        seed_urls=seed_urls,
        groups=groups,
        min_score=args.min_score,
        max_pages=args.max_pages,
        crawl_depth=args.crawl_depth,
        workers=args.workers,
        fetch_timeout=args.fetch_timeout,
        max_pages_per_domain=args.max_pages_per_domain,
        cache_db=cache_db,
        cache_ttl_hours=args.cache_ttl_hours,
        retries=args.retries,
        backoff_base=args.retry_backoff_base,
        host_interval_seconds=args.host_interval_seconds,
        robots_enabled=args.respect_robots,
        pdf_max_pages=args.pdf_max_pages,
        checkpoint_path=Path(args.checkpoint_path),
        checkpoint_every=args.checkpoint_every,
        resume=args.resume,
    )
    save_results(results, Path(args.output), groups)
    print(f"Saved {len(results)} matches to {args.output}.json and {args.output}.csv")


if __name__ == "__main__":
    main()
