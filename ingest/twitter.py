import tweepy
from datetime import datetime, timezone
from loguru import logger

import config
from db import insert_item, log_api_usage


def _get_client() -> tweepy.Client:
    return tweepy.Client(
        bearer_token=config.X_BEARER_TOKEN,
        consumer_key=config.X_API_KEY,
        consumer_secret=config.X_API_SECRET,
        access_token=config.X_ACCESS_TOKEN,
        access_token_secret=config.X_ACCESS_SECRET,
        wait_on_rate_limit=True,
    )


def _normalize_tweet(tweet, includes: dict, item_type: str) -> dict:
    author = None
    follower_count = 0
    if includes and "users" in includes:
        users_by_id = {u.id: u for u in includes["users"]}
        if tweet.author_id in users_by_id:
            user = users_by_id[tweet.author_id]
            author = user.username
            follower_count = getattr(user, "public_metrics", {}).get(
                "followers_count", 0
            )

    metrics = tweet.public_metrics or {}
    engagement = (
        metrics.get("like_count", 0)
        + metrics.get("retweet_count", 0)
        + metrics.get("reply_count", 0)
        + metrics.get("quote_count", 0)
    )

    return {
        "source": "x",
        "external_id": f"x_{tweet.id}",
        "text": tweet.text,
        "title": tweet.text[:120],
        "url": f"https://x.com/{author or 'i'}/status/{tweet.id}",
        "author": author or str(tweet.author_id),
        "engagement_count": engagement,
        "follower_count": follower_count,
        "created_at": tweet.created_at.isoformat() if tweet.created_at else "",
        "item_type": item_type,
        "raw": {"tweet_id": str(tweet.id), "metrics": metrics},
    }


def _search_tweets(client: tweepy.Client, query: str, item_type: str,
                   min_engagement: int = 0) -> list[dict]:
    items = []
    try:
        response = client.search_recent_tweets(
            query=query,
            max_results=100,
            tweet_fields=["created_at", "public_metrics", "author_id"],
            user_fields=["username", "public_metrics"],
            expansions=["author_id"],
        )
        log_api_usage("x", "search_recent_tweets")

        if not response.data:
            return items

        includes = response.includes or {}
        for tweet in response.data:
            item = _normalize_tweet(tweet, includes, item_type)
            if item["engagement_count"] >= min_engagement:
                items.append(item)

    except tweepy.TooManyRequests:
        logger.warning("X API rate limit hit, will retry next cycle")
    except tweepy.TweepyException as e:
        logger.error(f"X API error: {e}")

    return items


def _build_political_query() -> str:
    keyword_parts = [f'"{kw}"' for kw in config.KEYWORDS]
    keyword_query = " OR ".join(keyword_parts)
    account_parts = [f"from:{acct}" for acct in config.ACCOUNTS_TO_MONITOR]
    account_query = " OR ".join(account_parts)
    return f"({keyword_query} OR {account_query}) -is:retweet lang:en"


def _build_brand_query() -> str:
    parts = []
    for term in config.BRAND_TERMS:
        if term.startswith("@") or term.startswith("$"):
            parts.append(term)
        else:
            parts.append(f'"{term}"')
    return f"({' OR '.join(parts)}) lang:en"


def ingest():
    if not config.X_BEARER_TOKEN:
        logger.warning("X API credentials not configured, skipping Twitter ingest")
        return

    client = _get_client()
    inserted = 0

    # Political content search
    political_query = _build_political_query()
    logger.info(f"X political search: {political_query[:80]}...")
    political_items = _search_tweets(
        client, political_query, "political", config.MIN_ENGAGEMENT_POLITICAL
    )
    for item in political_items:
        if insert_item(item):
            inserted += 1

    # Brand monitoring search
    brand_query = _build_brand_query()
    logger.info(f"X brand search: {brand_query[:80]}...")
    brand_items = _search_tweets(
        client, brand_query, "brand", config.MIN_ENGAGEMENT_BRAND
    )
    for item in brand_items:
        if insert_item(item):
            inserted += 1

    logger.info(f"X ingest complete: {inserted} new items "
                f"({len(political_items)} political, {len(brand_items)} brand)")
