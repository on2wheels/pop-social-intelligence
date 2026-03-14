import feedparser
from datetime import datetime, timezone, timedelta
from time import mktime
from loguru import logger

import config
from db import insert_item, url_exists


def _parse_published(entry) -> datetime | None:
    for field in ("published_parsed", "updated_parsed"):
        parsed = getattr(entry, field, None) or entry.get(field)
        if parsed:
            try:
                return datetime.fromtimestamp(mktime(parsed), tz=timezone.utc)
            except (TypeError, ValueError, OverflowError):
                continue
    return None


def ingest():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    inserted = 0

    for feed_name, feed_url in config.RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)
            if feed.bozo and not feed.entries:
                logger.warning(f"RSS feed {feed_name} returned error: {feed.bozo_exception}")
                continue

            for entry in feed.entries:
                published = _parse_published(entry)
                if published and published < cutoff:
                    continue

                link = entry.get("link", "")

                # Skip if URL already ingested from any feed
                if link and url_exists(link):
                    continue

                external_id = f"rss_{feed_name}_{link or entry.get('id', entry.get('title', ''))}"

                text = entry.get("summary", entry.get("description", ""))
                if hasattr(text, "value"):
                    text = text.value

                item = {
                    "source": "rss",
                    "external_id": external_id[:200],
                    "text": text[:2000],
                    "title": entry.get("title", "")[:200],
                    "url": link,
                    "author": entry.get("author", feed_name),
                    "engagement_count": 0,
                    "follower_count": 0,
                    "created_at": published.isoformat() if published else "",
                    "item_type": "political",
                    "raw": {"feed": feed_name},
                }

                if insert_item(item):
                    inserted += 1

            logger.debug(f"RSS feed {feed_name}: processed {len(feed.entries)} entries")

        except Exception as e:
            logger.error(f"RSS feed {feed_name} error: {e}")

    logger.info(f"RSS ingest complete: {inserted} new items")
