import asyncio
import signal
import sys
import os
import json
from datetime import datetime, timezone, timedelta

import click
from loguru import logger

# Configure logging before imports
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")
logger.add(
    os.path.join(os.path.dirname(__file__), "logs", "errors.log"),
    level="ERROR",
    rotation="10 MB",
    retention="30 days",
)

from db import init_db, get_daily_spend, get_monthly_spend, get_db


@click.group()
def cli():
    """PoP Network Social Intelligence System"""
    init_db()


@cli.command()
@click.option("--source", type=click.Choice(["twitter", "reddit", "rss", "alerts", "web", "all"]),
              default="all")
def ingest(source):
    """Run ingestion from specified source(s)."""
    from ingest.twitter import ingest as twitter_ingest
    from ingest.reddit import ingest as reddit_ingest
    from ingest.rss import ingest as rss_ingest
    from ingest.alerts import ingest as alerts_ingest
    from ingest.web_search_monitor import ingest as web_ingest

    sources = {
        "twitter": twitter_ingest,
        "reddit": reddit_ingest,
        "rss": rss_ingest,
        "alerts": alerts_ingest,
        "web": web_ingest,
    }

    if source == "all":
        for name, func in sources.items():
            logger.info(f"Running {name} ingest...")
            func()
    else:
        logger.info(f"Running {source} ingest...")
        sources[source]()


@cli.command()
@click.option("--type", "process_type",
              type=click.Choice(["filter", "brand", "evaluate", "generate", "all"]),
              default="all")
def process(process_type):
    """Run processing pipeline."""
    from process.filter import process_pending as filter_pending
    from process.brand_analyze import process_pending as brand_pending
    from process.evaluate import process_pending as evaluate_pending
    from process.generate import process_pending as generate_pending

    processors = {
        "filter": filter_pending,
        "brand": brand_pending,
        "evaluate": evaluate_pending,
        "generate": generate_pending,
    }

    if process_type == "all":
        for name, func in processors.items():
            logger.info(f"Running {name}...")
            func()
    else:
        logger.info(f"Running {process_type}...")
        processors[process_type]()


@cli.command()
@click.option("--type", "digest_type",
              type=click.Choice(["opportunities", "brand"]),
              required=True)
@click.option("--dry-run", is_flag=True, help="Print digest to console instead of sending")
def digest(digest_type, dry_run):
    """Send a digest via Telegram (or print with --dry-run)."""
    if dry_run:
        if digest_type == "opportunities":
            from db import get_pending_opportunities
            opps = get_pending_opportunities(5, min_score=6.5)
            for opp in opps:
                options = json.loads(opp.get("options_json", "[]"))
                click.echo(f"\n{'='*60}")
                click.echo(f"[{opp.get('source', '?').upper()}] Score: {opp.get('score', '?')}")
                click.echo(f"Text: {(opp.get('item_text') or '')[:100]}")
                for opt in options:
                    click.echo(f"\n--- Option {opt.get('option', '?')} — {opt.get('label', '?')} ---")
                    click.echo(opt.get("draft_text", ""))
            if not opps:
                click.echo("No pending opportunities.")
        else:
            from db import get_pending_brand_mentions
            mentions = get_pending_brand_mentions()
            click.echo(f"Brand mentions: {len(mentions)}")
            for m in mentions[:10]:
                click.echo(f"\n  @{m.get('author', '?')} [{m.get('sentiment', '?')}] "
                           f"{(m.get('mention_text') or '')[:80]}")
            if not mentions:
                click.echo("No pending brand mentions.")
    else:
        from interface.telegram_bot import build_app, send_opportunity_digest, send_brand_digest

        async def _send():
            app = build_app()
            async with app:
                if digest_type == "opportunities":
                    await send_opportunity_digest(app)
                else:
                    await send_brand_digest(app)

        asyncio.run(_send())


@cli.command()
def serve():
    """Start the scheduler and Telegram bot (main production command)."""
    from interface.telegram_bot import build_app
    from scheduler import create_scheduler

    logger.info("Starting PoP Social Intelligence System...")

    app = build_app()
    scheduler = create_scheduler(telegram_app=app)

    async def _run():
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

        async with app:
            scheduler.start()
            logger.info("Scheduler started. Bot is running. Press Ctrl+C to stop.")
            await app.initialize()
            await app.start()
            await app.updater.start_polling()
            await stop_event.wait()
            logger.info("Shutting down...")
            scheduler.shutdown()
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    asyncio.run(_run())


@cli.command()
@click.option("--type", "report_type",
              type=click.Choice(["brand", "decisions"]),
              required=True)
@click.option("--week", default=None, help="Week start date YYYY-MM-DD (default: this week)")
def report(report_type, week):
    """Generate weekly report."""
    if week:
        week_start = datetime.strptime(week, "%Y-%m-%d")
    else:
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday())

    week_str = week_start.strftime("%Y-%m-%d")

    if report_type == "brand":
        _generate_brand_report(week_str)
    else:
        _generate_decision_report(week_str)


def _generate_brand_report(week_str: str):
    with get_db() as conn:
        stats = conn.execute(
            """SELECT
                COUNT(*) as total,
                COUNT(DISTINCT author) as unique_authors,
                SUM(CASE WHEN is_first_mention THEN 1 ELSE 0 END) as first_timers,
                SUM(CASE WHEN sentiment='positive' THEN 1 ELSE 0 END) as pos,
                SUM(CASE WHEN sentiment='neutral' THEN 1 ELSE 0 END) as neu,
                SUM(CASE WHEN sentiment='negative' THEN 1 ELSE 0 END) as neg
            FROM brand_mentions
            WHERE ingested_at >= ? AND ingested_at < date(?, '+7 days')""",
            (week_str, week_str),
        ).fetchone()

        prev = conn.execute(
            """SELECT COUNT(*) as total FROM brand_mentions
            WHERE ingested_at >= date(?, '-7 days') AND ingested_at < ?""",
            (week_str, week_str),
        ).fetchone()

        top_reach = conn.execute(
            """SELECT author, mention_text, url, reach FROM brand_mentions
            WHERE ingested_at >= ? AND ingested_at < date(?, '+7 days')
            ORDER BY reach DESC LIMIT 5""",
            (week_str, week_str),
        ).fetchall()

        first_timers = conn.execute(
            """SELECT author, url, context_type FROM brand_mentions
            WHERE ingested_at >= ? AND ingested_at < date(?, '+7 days')
            AND is_first_mention = TRUE
            ORDER BY reach DESC LIMIT 10""",
            (week_str, week_str),
        ).fetchall()

        outstanding = conn.execute(
            """SELECT author, mention_text, url FROM brand_mentions
            WHERE response_warranted = 'yes' AND reviewed = FALSE
            ORDER BY reach DESC LIMIT 5"""
        ).fetchall()

        # Source breakdown
        sources = conn.execute(
            """SELECT s.source, COUNT(*) as cnt FROM brand_mentions b
            JOIN seen_items s ON b.item_id = s.id
            WHERE b.ingested_at >= ? AND b.ingested_at < date(?, '+7 days')
            GROUP BY s.source""",
            (week_str, week_str),
        ).fetchall()

    total = stats["total"]
    prev_total = prev["total"]
    delta_pct = ((total - prev_total) / prev_total * 100) if prev_total else 0

    lines = [
        f"# PoP Network Brand Report — Week of {week_str}\n",
        "## Mention Volume",
        f"- Total: {total} (vs {prev_total} last week, {delta_pct:+.0f}%)",
        f"- Unique authors: {stats['unique_authors']}",
        f"- First-time mentioners: {stats['first_timers']}\n",
        "## Sentiment",
        f"- Positive: {_pct(stats['pos'], total)}% · Neutral: {_pct(stats['neu'], total)}% · Negative: {_pct(stats['neg'], total)}%\n",
        "## Source Breakdown",
    ]
    for s in sources:
        lines.append(f"- {s['source']}: {s['cnt']}")

    lines.append("\n## Highest Reach")
    for r in top_reach:
        lines.append(f"- {r['author']}: \"{(r['mention_text'] or '')[:60]}\" ({r['reach']} reach) — {r['url']}")

    lines.append("\n## First-Time Mentioners (engage these)")
    for f in first_timers:
        lines.append(f"- {f['author']} ({f['context_type']}) — {f['url']}")

    lines.append("\n## Action Items Outstanding")
    for o in outstanding:
        lines.append(f"- {o['author']}: \"{(o['mention_text'] or '')[:60]}\" — {o['url']}")
    if not outstanding:
        lines.append("- None")

    filename = f"BRAND_REPORT_{week_str}.md"
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "w") as fp:
        fp.write("\n".join(lines))
    click.echo(f"Brand report written to {filename}")


def _generate_decision_report(week_str: str):
    with get_db() as conn:
        option_counts = conn.execute(
            """SELECT selected_option, COUNT(*) as cnt FROM decisions
            WHERE decided_at >= ? AND decided_at < date(?, '+7 days')
            AND action = 'approved'
            GROUP BY selected_option ORDER BY cnt DESC""",
            (week_str, week_str),
        ).fetchall()

        moment_counts = conn.execute(
            """SELECT o.moment_type, COUNT(*) as cnt FROM decisions d
            JOIN opportunities o ON d.opportunity_id = o.id
            WHERE d.decided_at >= ? AND d.decided_at < date(?, '+7 days')
            AND d.action = 'approved'
            GROUP BY o.moment_type ORDER BY cnt DESC""",
            (week_str, week_str),
        ).fetchall()

        source_counts = conn.execute(
            """SELECT s.source, COUNT(*) as cnt FROM decisions d
            JOIN opportunities o ON d.opportunity_id = o.id
            JOIN seen_items s ON o.item_id = s.id
            WHERE d.decided_at >= ? AND d.decided_at < date(?, '+7 days')
            AND d.action = 'approved'
            GROUP BY s.source ORDER BY cnt DESC""",
            (week_str, week_str),
        ).fetchall()

        skip_rates = conn.execute(
            """SELECT s.source,
                SUM(CASE WHEN d.action='skipped' THEN 1 ELSE 0 END) as skipped,
                COUNT(*) as total
            FROM decisions d
            JOIN opportunities o ON d.opportunity_id = o.id
            JOIN seen_items s ON o.item_id = s.id
            WHERE d.decided_at >= ? AND d.decided_at < date(?, '+7 days')
            GROUP BY s.source""",
            (week_str, week_str),
        ).fetchall()

    lines = [
        f"# PoP Network Decision Report — Week of {week_str}\n",
        "## Option Types Approved",
    ]
    for o in option_counts:
        lines.append(f"- Option {o['selected_option']}: {o['cnt']}")

    lines.append("\n## Moment Types Approved")
    for m in moment_counts:
        lines.append(f"- {m['moment_type']}: {m['cnt']}")

    lines.append("\n## Sources Producing Approved Content")
    for s in source_counts:
        lines.append(f"- {s['source']}: {s['cnt']}")

    lines.append("\n## Skip Rate by Source")
    for s in skip_rates:
        rate = (s["skipped"] / s["total"] * 100) if s["total"] else 0
        lines.append(f"- {s['source']}: {rate:.0f}% ({s['skipped']}/{s['total']})")

    filename = f"DECISION_REPORT_{week_str}.md"
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "w") as fp:
        fp.write("\n".join(lines))
    click.echo(f"Decision report written to {filename}")


def _pct(part, total):
    return round(part / total * 100) if total else 0


@cli.command()
def budget():
    """Show current LLM spend vs limits."""
    daily = get_daily_spend()
    monthly = get_monthly_spend()
    click.echo(f"Daily:   ${daily:.2f} / ${config.DAILY_LLM_BUDGET_USD:.2f}")
    click.echo(f"Monthly: ${monthly:.2f} / ${config.MONTHLY_LLM_BUDGET_USD:.2f}")


if __name__ == "__main__":
    cli()
