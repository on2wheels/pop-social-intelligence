import praw
from datetime import datetime, timezone, timedelta
from loguru import logger

import config
from db import insert_item, log_api_usage


def _get_reddit() -> praw.Reddit:
    return praw.Reddit(
        client_id=config.REDDIT_CLIENT_ID,
        client_secret=config.REDDIT_CLIENT_SECRET,
        user_agent=config.REDDIT_USER_AGENT,
    )


def _normalize_post(submission, item_type: str, extra_text: str = "") -> dict:
    text = submission.selftext or submission.title
    if extra_text:
        text = f"{text}\n\n---TOP COMMENTS---\n{extra_text}"

    return {
        "source": "reddit",
        "external_id": f"reddit_{submission.id}",
        "text": text,
        "title": submission.title,
        "url": f"https://reddit.com{submission.permalink}",
        "author": f"u/{submission.author.name if submission.author else '[deleted]'} in r/{submission.subreddit.display_name}",
        "engagement_count": submission.score + submission.num_comments,
        "follower_count": 0,
        "created_at": datetime.fromtimestamp(
            submission.created_utc, tz=timezone.utc
        ).isoformat(),
        "item_type": item_type,
        "raw": {
            "subreddit": submission.subreddit.display_name,
            "score": submission.score,
            "num_comments": submission.num_comments,
        },
    }


def _get_top_comments(submission, n: int = 3) -> str:
    submission.comment_sort = "best"
    submission.comments.replace_more(limit=0)
    comments = []
    for comment in submission.comments[:n]:
        if hasattr(comment, "body"):
            comments.append(comment.body[:300])
    return "\n---\n".join(comments)


def _ingest_subreddits(reddit: praw.Reddit) -> int:
    inserted = 0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=6)

    for sub_name in config.SUBREDDITS:
        try:
            subreddit = reddit.subreddit(sub_name)
            for submission in subreddit.hot(limit=25):
                created = datetime.fromtimestamp(
                    submission.created_utc, tz=timezone.utc
                )
                if created < cutoff:
                    continue

                top_comments = _get_top_comments(submission)
                item = _normalize_post(submission, "political", top_comments)
                if insert_item(item):
                    inserted += 1

            log_api_usage("reddit", f"subreddit/{sub_name}")
        except Exception as e:
            logger.error(f"Reddit error for r/{sub_name}: {e}")

    return inserted


def _ingest_brand_search(reddit: praw.Reddit) -> int:
    inserted = 0

    for term in config.BRAND_TERMS:
        try:
            for submission in reddit.subreddit("all").search(
                term, sort="new", time_filter="week", limit=25
            ):
                item = _normalize_post(submission, "brand")
                if insert_item(item):
                    inserted += 1

            log_api_usage("reddit", f"search/{term}")
        except Exception as e:
            logger.error(f"Reddit brand search error for '{term}': {e}")

    return inserted


def ingest():
    if not config.REDDIT_CLIENT_ID:
        logger.warning("Reddit credentials not configured, skipping Reddit ingest")
        return

    reddit = _get_reddit()
    political = _ingest_subreddits(reddit)
    brand = _ingest_brand_search(reddit)
    logger.info(f"Reddit ingest complete: {political} political, {brand} brand")
