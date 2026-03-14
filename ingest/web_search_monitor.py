import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import quote_plus
from loguru import logger

import config
from db import insert_item


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def _search_ddg(query: str) -> list[dict]:
    results = []
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for result in soup.select(".result"):
            title_el = result.select_one(".result__title a")
            snippet_el = result.select_one(".result__snippet")
            if not title_el:
                continue

            href = title_el.get("href", "")
            # DDG wraps URLs — extract actual URL
            if "uddg=" in href:
                from urllib.parse import parse_qs, urlparse
                parsed = urlparse(href)
                params = parse_qs(parsed.query)
                href = params.get("uddg", [href])[0]

            results.append({
                "title": title_el.get_text(strip=True),
                "url": href,
                "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
            })

    except requests.RequestException as e:
        logger.error(f"DuckDuckGo search error for '{query}': {e}")

    return results


def ingest():
    inserted = 0
    seen_urls = set()

    for term in config.BRAND_TERMS:
        results = _search_ddg(term)

        for result in results:
            url = result["url"]
            if url in seen_urls or not url.startswith("http"):
                continue
            seen_urls.add(url)

            external_id = f"web_{url[:180]}"

            item = {
                "source": "web",
                "external_id": external_id,
                "text": f"{result['title']}\n\n{result['snippet']}",
                "title": result["title"][:200],
                "url": url,
                "author": "web search",
                "engagement_count": 0,
                "follower_count": 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "item_type": "brand",
                "raw": {"search_term": term},
            }

            if insert_item(item):
                inserted += 1

    logger.info(f"Web search ingest complete: {inserted} new items from {len(seen_urls)} URLs")
