import json
import os
from datetime import datetime, timezone
from loguru import logger
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes,
)

import config
from db import (
    get_pending_opportunities, get_pending_brand_mentions,
    get_opportunity_by_id, insert_decision, mark_brand_reviewed,
    mark_posted, get_approved_not_posted, get_brand_stats_week,
    get_daily_spend, get_monthly_spend,
)


# --- Digest senders (called by scheduler) ---

async def send_opportunity_digest(app: Application | None = None):
    """Send top 5 opportunities as Telegram messages with action buttons."""
    if app is None:
        logger.warning("No app provided for opportunity digest")
        return

    bot = app.bot
    chat_id = config.TELEGRAM_CHAT_ID
    opps = get_pending_opportunities(limit=5, min_score=6.5)

    if not opps:
        await bot.send_message(chat_id, "No new opportunities in the last 24h.")
        return

    for opp in opps:
        options = json.loads(opp.get("options_json", "[]"))
        source = opp.get("source", "?").upper()
        moment = (opp.get("moment_type") or "?").upper()
        item_text = (opp.get("item_text") or "")[:100]
        timing = opp.get("timing_recommendation", "?")

        lines = [
            f"🔍 [{source}] · [{moment}]",
            f"📋 {item_text}",
            f"⏰ {timing}",
            "",
        ]

        for opt in options:
            label = opt.get("label", opt.get("option", "?"))
            draft = opt.get("draft_text", "")
            manual = "⚠️ Manual poll creation required\n" if opt.get("requires_manual_action") else ""
            lines.append(f"━━━ OPTION {opt.get('option', '?')} — {label} ━━━")
            lines.append(draft)
            if manual:
                lines.append(manual)
            lines.append("")

        text = "\n".join(lines)

        buttons = [
            [
                InlineKeyboardButton("✅ A", callback_data=f"approve:{opp['id']}:A"),
                InlineKeyboardButton("✅ B", callback_data=f"approve:{opp['id']}:B"),
                InlineKeyboardButton("✅ C", callback_data=f"approve:{opp['id']}:C"),
            ],
            [
                InlineKeyboardButton("✏️ Edit", callback_data=f"edit:{opp['id']}"),
                InlineKeyboardButton("⏭️ Skip", callback_data=f"skip:{opp['id']}"),
            ],
        ]

        await bot.send_message(
            chat_id, text[:4096],
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    logger.info(f"Sent opportunity digest with {len(opps)} items")


async def send_brand_digest(app: Application | None = None):
    """Send brand mention summary + notable mentions with action buttons."""
    if app is None:
        logger.warning("No app provided for brand digest")
        return

    bot = app.bot
    chat_id = config.TELEGRAM_CHAT_ID
    mentions = get_pending_brand_mentions()

    if not mentions:
        await bot.send_message(chat_id, "📡 No new brand mentions in the last 24h.")
        return

    # Summary stats
    total = len(mentions)
    new_authors = sum(1 for m in mentions if m.get("is_first_mention"))
    positive = sum(1 for m in mentions if m.get("sentiment") == "positive")
    neutral = sum(1 for m in mentions if m.get("sentiment") == "neutral")
    negative = sum(1 for m in mentions if m.get("sentiment") == "negative")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    header = (
        f"📡 BRAND MENTIONS — {today}\n\n"
        f"📊 Total: {total} · New authors: {new_authors}\n"
        f"😊 {positive}+ · 😐 {neutral}~ · 😠 {negative}-\n\n"
        f"━━━ NOTABLE ━━━\n"
    )

    await bot.send_message(chat_id, header)

    # Filter for notable mentions
    notable_types = {"organic_discovery", "press", "influencer", "skeptical"}
    notable = [
        m for m in mentions
        if m.get("context_type") in notable_types or m.get("is_first_mention")
    ]

    for idx, mention in enumerate(notable[:10], 1):
        author = mention.get("author", "unknown")
        reach = mention.get("reach", 0)
        ctx = mention.get("context_type", "?")
        sentiment = mention.get("sentiment", "?")
        text = (mention.get("mention_text") or "")[:120]
        url = mention.get("url", "")

        msg = (
            f"[{idx}/{len(notable)}] @{author} ({reach} followers)\n"
            f"{ctx} · {sentiment}\n"
            f'"{text}"\n'
            f"{url}"
        )

        buttons = [
            [
                InlineKeyboardButton("💬 Respond", callback_data=f"brand_respond:{mention['id']}"),
                InlineKeyboardButton("👁 Monitor", callback_data=f"brand_monitor:{mention['id']}"),
                InlineKeyboardButton("⏭️ Skip", callback_data=f"brand_skip:{mention['id']}"),
            ]
        ]

        await bot.send_message(
            chat_id, msg[:4096],
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    # Mark routine non-notable as reviewed
    for m in mentions:
        if m not in notable:
            mark_brand_reviewed(m["id"])

    logger.info(f"Sent brand digest: {total} total, {len(notable)} notable")


# --- Fallback file digests ---

def write_digest_fallback(digest_type: str, content: str):
    filename = f"{'DAILY' if digest_type == 'opportunities' else 'BRAND'}_DIGEST.md"
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), filename)
    with open(path, "w") as f:
        f.write(content)
    logger.info(f"Wrote fallback digest to {path}")


# --- Callback handlers ---

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    logger.info(f"Callback received: data={data!r} from user={query.from_user.id}")

    if data.startswith("approve:"):
        parts = data.split(":")
        if len(parts) != 3:
            logger.error(f"Malformed approve callback_data: {data!r}")
            await query.edit_message_text("❌ Error: malformed callback data")
            return
        _, opp_id_str, option = parts
        opp_id = int(opp_id_str)
        logger.info(f"Approve action: opp_id={opp_id}, option={option!r}")

        # Look up the opportunity directly by ID (not via pending filter)
        opp = get_opportunity_by_id(opp_id)
        if not opp:
            logger.error(f"Opportunity {opp_id} not found in DB")
            await query.edit_message_text("❌ Opportunity not found")
            return

        options = json.loads(opp.get("options_json", "[]"))
        selected = next((o for o in options if o.get("option") == option), None)
        if not selected:
            logger.error(f"Option {option!r} not found in options_json for opp {opp_id}")
            await query.edit_message_text(f"❌ Option {option} not found")
            return

        final_text = selected.get("draft_text", "")
        logger.info(f"Selected option {option} text: {final_text[:80]!r}")

        insert_decision({
            "opportunity_id": opp_id,
            "decision_type": "opportunity",
            "action": "approved",
            "selected_option": option,
            "final_text": final_text,
        })
        logger.info(f"Decision inserted for opp {opp_id}, option {option}")

        # Write to APPROVED_POSTS.txt
        posts_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "APPROVED_POSTS.txt")
        with open(posts_path, "a") as f:
            f.write(f"\n---\n[{datetime.now(timezone.utc).isoformat()}] Option {option}\n{final_text}\n")
        logger.info(f"Wrote approved post to {posts_path}")

        await query.edit_message_text(f"✅ Queued option {option}: {final_text[:50]}...")

    elif data.startswith("edit:"):
        opp_id = int(data.split(":")[1])
        context.user_data["editing_opp_id"] = opp_id
        await query.edit_message_text("Send your edited text:")

    elif data.startswith("skip:"):
        opp_id = int(data.split(":")[1])
        insert_decision({
            "opportunity_id": opp_id,
            "decision_type": "opportunity",
            "action": "skipped",
        })
        await query.edit_message_text("⏭️ Skipped")

    elif data.startswith("brand_respond:"):
        mention_id = int(data.split(":")[1])
        insert_decision({
            "opportunity_id": mention_id,
            "decision_type": "brand",
            "action": "respond",
        })
        mark_brand_reviewed(mention_id)
        await query.edit_message_text("💬 Flagged for response")

    elif data.startswith("brand_monitor:"):
        mention_id = int(data.split(":")[1])
        insert_decision({
            "opportunity_id": mention_id,
            "decision_type": "brand",
            "action": "monitor",
        })
        mark_brand_reviewed(mention_id)
        await query.edit_message_text("👁 Monitoring")

    elif data.startswith("brand_skip:"):
        mention_id = int(data.split(":")[1])
        mark_brand_reviewed(mention_id)
        await query.edit_message_text("⏭️ Skipped")

    elif data.startswith("mark_posted:"):
        decision_id = int(data.split(":")[1])
        mark_posted(decision_id)
        await query.edit_message_text("✅ Marked as posted")


async def handle_edited_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    opp_id = context.user_data.get("editing_opp_id")
    if not opp_id:
        return

    edited_text = update.message.text
    insert_decision({
        "opportunity_id": opp_id,
        "decision_type": "opportunity",
        "action": "edited",
        "final_text": edited_text,
    })

    posts_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "APPROVED_POSTS.txt")
    with open(posts_path, "a") as f:
        f.write(f"\n---\n[{datetime.now(timezone.utc).isoformat()}] Edited\n{edited_text}\n")

    context.user_data.pop("editing_opp_id", None)
    await update.message.reply_text(f"✅ Saved edit: {edited_text[:50]}...")


# --- Bot commands ---

async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    items = get_approved_not_posted()
    if not items:
        await update.message.reply_text("📭 Queue is empty — all posts have been marked as posted.")
        return

    for item in items:
        text = (item.get("final_text") or "")[:200]
        buttons = [[
            InlineKeyboardButton("✅ Mark Posted", callback_data=f"mark_posted:{item['id']}")
        ]]
        await update.message.reply_text(
            f"📝 {text}",
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def cmd_mentions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = get_brand_stats_week()
    tw = stats["this_week"]
    lw_total = stats["last_week_total"]

    delta = ""
    if lw_total > 0:
        pct = ((tw["total"] - lw_total) / lw_total) * 100
        delta = f" ({pct:+.0f}% vs last week)"

    msg = (
        f"📊 Brand Mentions — This Week\n\n"
        f"Total: {tw['total']}{delta}\n"
        f"Unique authors: {tw['unique_authors']}\n"
        f"First-time mentioners: {tw['first_timers']}\n\n"
        f"Sentiment:\n"
        f"  😊 Positive: {tw['positive']}\n"
        f"  😐 Neutral: {tw['neutral']}\n"
        f"  😠 Negative: {tw['negative']}\n"
    )
    await update.message.reply_text(msg)


async def cmd_budget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    daily = get_daily_spend()
    monthly = get_monthly_spend()
    msg = (
        f"💰 LLM Budget\n\n"
        f"Today: ${daily:.2f} / ${config.DAILY_LLM_BUDGET_USD:.2f}\n"
        f"This month: ${monthly:.2f} / ${config.MONTHLY_LLM_BUDGET_USD:.2f}\n"
    )
    await update.message.reply_text(msg)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from db import get_db
    with get_db() as conn:
        sources = conn.execute(
            """SELECT source, item_type,
                      COUNT(*) as count,
                      MAX(ingested_at) as last_ingest
               FROM seen_items
               GROUP BY source, item_type
               ORDER BY last_ingest DESC"""
        ).fetchall()

        errors = conn.execute(
            """SELECT COUNT(*) as count FROM seen_items
               WHERE ingested_at >= date('now')"""
        ).fetchone()

    lines = ["📊 System Status\n"]
    for s in sources:
        lines.append(f"  {s['source']}/{s['item_type']}: {s['count']} items, last: {s['last_ingest']}")

    lines.append(f"\nItems ingested today: {errors['count']}")
    await update.message.reply_text("\n".join(lines))


# --- App builder ---

def build_app() -> Application:
    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("queue", cmd_queue))
    app.add_handler(CommandHandler("mentions", cmd_mentions))
    app.add_handler(CommandHandler("budget", cmd_budget))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_edited_text
    ))

    return app
