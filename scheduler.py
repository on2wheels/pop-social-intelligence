import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

import config
from db import get_daily_spend, get_monthly_spend
from ingest.twitter import ingest as twitter_ingest
from ingest.reddit import ingest as reddit_ingest
from ingest.rss import ingest as rss_ingest
from ingest.alerts import ingest as alerts_ingest
from ingest.web_search_monitor import ingest as web_ingest
from process.filter import process_pending as filter_pending
from process.brand_analyze import process_pending as brand_pending
from process.evaluate import process_pending as evaluate_pending
from process.generate import process_pending as generate_pending
from interface.telegram_bot import send_opportunity_digest, send_brand_digest


def _check_budget_and_run(func):
    """Wrapper that checks budget before running LLM-dependent tasks."""
    def wrapper():
        daily = get_daily_spend()
        monthly = get_monthly_spend()
        if daily >= config.DAILY_LLM_BUDGET_USD:
            logger.warning(f"Daily budget exceeded (${daily:.2f}), skipping {func.__name__}")
            return
        if monthly >= config.MONTHLY_LLM_BUDGET_USD:
            logger.warning(f"Monthly budget exceeded (${monthly:.2f}), skipping {func.__name__}")
            return
        func()
    wrapper.__name__ = func.__name__
    return wrapper


def create_scheduler(telegram_app=None) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="America/Los_Angeles")

    # --- Ingest jobs ---
    scheduler.add_job(twitter_ingest, IntervalTrigger(minutes=30), id="twitter_ingest",
                      name="Twitter ingest", misfire_grace_time=300)
    scheduler.add_job(reddit_ingest, IntervalTrigger(hours=3), id="reddit_ingest",
                      name="Reddit ingest", misfire_grace_time=300)
    scheduler.add_job(rss_ingest, IntervalTrigger(hours=3), id="rss_ingest",
                      name="RSS ingest", misfire_grace_time=300)
    scheduler.add_job(alerts_ingest, IntervalTrigger(hours=1), id="alerts_ingest",
                      name="Gmail alerts ingest", misfire_grace_time=300)
    scheduler.add_job(web_ingest, CronTrigger(hour=6), id="web_ingest",
                      name="Web search ingest", misfire_grace_time=3600)

    # --- Process jobs ---
    scheduler.add_job(_check_budget_and_run(filter_pending),
                      IntervalTrigger(hours=1), id="filter",
                      name="Political filter", misfire_grace_time=300)
    scheduler.add_job(_check_budget_and_run(brand_pending),
                      IntervalTrigger(hours=1), id="brand_analyze",
                      name="Brand analyze", misfire_grace_time=300)
    scheduler.add_job(_check_budget_and_run(evaluate_pending),
                      IntervalTrigger(hours=1), id="evaluate",
                      name="Evaluate opportunities", misfire_grace_time=300)
    scheduler.add_job(_check_budget_and_run(generate_pending),
                      IntervalTrigger(hours=1), id="generate",
                      name="Generate options", misfire_grace_time=300)

    # --- Digest jobs ---
    if telegram_app:
        async def _send_opp_digest():
            await send_opportunity_digest(telegram_app)

        async def _send_brand_digest():
            await send_brand_digest(telegram_app)

        scheduler.add_job(_send_opp_digest,
                          CronTrigger(hour=config.OPPORTUNITY_DIGEST_HOUR),
                          id="opp_digest", name="Opportunity digest")
        scheduler.add_job(_send_brand_digest,
                          CronTrigger(hour=config.BRAND_DIGEST_HOUR),
                          id="brand_digest", name="Brand digest")

    return scheduler
