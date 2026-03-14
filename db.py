import sqlite3
import json
from datetime import datetime, timezone
from contextlib import contextmanager

from config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    external_id TEXT UNIQUE NOT NULL,
    title TEXT,
    url TEXT,
    text TEXT,
    author TEXT,
    engagement_count INTEGER DEFAULT 0,
    follower_count INTEGER DEFAULT 0,
    raw_json TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    item_type TEXT NOT NULL,
    processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER REFERENCES seen_items(id),
    score REAL,
    moment_type TEXT,
    risk_flags TEXT,
    structural_observation TEXT,
    pop_angle TEXT,
    format_recommendation TEXT,
    platform_recommendation TEXT,
    timing_recommendation TEXT,
    evaluation_rationale TEXT,
    options_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS brand_mentions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER REFERENCES seen_items(id),
    brand_term TEXT,
    mention_text TEXT,
    author TEXT,
    url TEXT,
    sentiment TEXT,
    context_type TEXT,
    reach INTEGER DEFAULT 0,
    is_first_mention BOOLEAN DEFAULT FALSE,
    response_warranted TEXT DEFAULT 'no',
    suggested_response TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id INTEGER,
    decision_type TEXT,
    action TEXT,
    selected_option TEXT,
    final_text TEXT,
    decided_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    posted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS llm_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cost_usd REAL,
    purpose TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS api_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT,
    endpoint TEXT,
    request_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def init_db():
    with get_db() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def item_exists(external_id: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM seen_items WHERE external_id = ?", (external_id,)
        ).fetchone()
        return row is not None


def url_exists(url: str) -> bool:
    """Return True if a non-empty URL is already in seen_items."""
    if not url:
        return False
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM seen_items WHERE url = ?", (url,)
        ).fetchone()
        return row is not None


def insert_item(item: dict) -> int | None:
    if item_exists(item["external_id"]):
        return None
    if url_exists(item.get("url", "")):
        return None
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO seen_items
            (source, external_id, title, url, text, author,
             engagement_count, follower_count, raw_json, item_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                item["source"],
                item["external_id"],
                item.get("title", ""),
                item.get("url", ""),
                item.get("text", ""),
                item.get("author", ""),
                item.get("engagement_count", 0),
                item.get("follower_count", 0),
                json.dumps(item.get("raw", {})),
                item["item_type"],
            ),
        )
        return cur.lastrowid


def get_pending_items(item_type: str, limit: int = 50) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM seen_items
            WHERE item_type = ? AND processed = FALSE
            ORDER BY ingested_at DESC LIMIT ?""",
            (item_type, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def mark_processed(item_ids: list[int]):
    with get_db() as conn:
        conn.executemany(
            "UPDATE seen_items SET processed = TRUE WHERE id = ?",
            [(i,) for i in item_ids],
        )


def insert_opportunity(opp: dict) -> int:
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO opportunities
            (item_id, score, moment_type, risk_flags,
             structural_observation, pop_angle, format_recommendation,
             platform_recommendation, timing_recommendation,
             evaluation_rationale, options_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                opp["item_id"],
                opp.get("score", 0),
                opp.get("moment_type", ""),
                json.dumps(opp.get("risk_flags", {})),
                opp.get("structural_observation", ""),
                opp.get("pop_angle", ""),
                opp.get("format_recommendation", ""),
                opp.get("platform_recommendation", ""),
                opp.get("timing_recommendation", ""),
                opp.get("evaluation_rationale", ""),
                json.dumps(opp.get("options", [])),
            ),
        )
        return cur.lastrowid


def insert_brand_mention(mention: dict) -> int:
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO brand_mentions
            (item_id, brand_term, mention_text, author, url,
             sentiment, context_type, reach, is_first_mention,
             response_warranted, suggested_response)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                mention["item_id"],
                mention.get("brand_term", ""),
                mention.get("mention_text", ""),
                mention.get("author", ""),
                mention.get("url", ""),
                mention.get("sentiment", "unknown"),
                mention.get("context_type", ""),
                mention.get("reach", 0),
                mention.get("is_first_mention", False),
                mention.get("response_warranted", "no"),
                mention.get("suggested_response"),
            ),
        )
        return cur.lastrowid


def is_first_mention(author: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM brand_mentions WHERE author = ?", (author,)
        ).fetchone()
        return row is None


def insert_decision(decision: dict) -> int:
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO decisions
            (opportunity_id, decision_type, action, selected_option, final_text)
            VALUES (?, ?, ?, ?, ?)""",
            (
                decision["opportunity_id"],
                decision["decision_type"],
                decision["action"],
                decision.get("selected_option"),
                decision.get("final_text"),
            ),
        )
        return cur.lastrowid


def get_recent_decisions(limit: int = 20) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT d.*, o.moment_type, o.score
            FROM decisions d
            LEFT JOIN opportunities o ON d.opportunity_id = o.id
            ORDER BY d.decided_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_opportunity_by_id(opp_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            """SELECT o.*, s.text as item_text, s.source, s.url as item_url
            FROM opportunities o
            JOIN seen_items s ON o.item_id = s.id
            WHERE o.id = ?""",
            (opp_id,),
        ).fetchone()
        return dict(row) if row else None


def get_pending_opportunities(limit: int = 5, min_score: float = 0) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT o.*, s.text as item_text, s.source, s.url as item_url
            FROM opportunities o
            JOIN seen_items s ON o.item_id = s.id
            WHERE o.id NOT IN (SELECT opportunity_id FROM decisions WHERE opportunity_id IS NOT NULL)
            AND o.options_json IS NOT NULL
            AND o.options_json != '[]'
            AND o.score >= ?
            ORDER BY o.score DESC LIMIT ?""",
            (min_score, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def get_pending_brand_mentions() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM brand_mentions
            WHERE reviewed = FALSE
            AND ingested_at >= datetime('now', '-24 hours')
            ORDER BY reach DESC"""
        ).fetchall()
        return [dict(r) for r in rows]


def mark_brand_reviewed(mention_id: int):
    with get_db() as conn:
        conn.execute(
            "UPDATE brand_mentions SET reviewed = TRUE WHERE id = ?",
            (mention_id,),
        )


def mark_posted(decision_id: int):
    with get_db() as conn:
        conn.execute(
            "UPDATE decisions SET posted_at = CURRENT_TIMESTAMP WHERE id = ?",
            (decision_id,),
        )


def get_approved_not_posted() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT d.*, o.moment_type
            FROM decisions d
            LEFT JOIN opportunities o ON d.opportunity_id = o.id
            WHERE d.action IN ('approved', 'respond')
            AND d.posted_at IS NULL
            ORDER BY d.decided_at ASC"""
        ).fetchall()
        return [dict(r) for r in rows]


def log_llm_usage(model: str, input_tokens: int, output_tokens: int,
                  cost_usd: float, purpose: str):
    with get_db() as conn:
        conn.execute(
            """INSERT INTO llm_usage (model, input_tokens, output_tokens, cost_usd, purpose)
            VALUES (?, ?, ?, ?, ?)""",
            (model, input_tokens, output_tokens, cost_usd, purpose),
        )


def get_daily_spend() -> float:
    with get_db() as conn:
        row = conn.execute(
            """SELECT COALESCE(SUM(cost_usd), 0) as total
            FROM llm_usage
            WHERE created_at >= date('now')"""
        ).fetchone()
        return row["total"]


def get_monthly_spend() -> float:
    with get_db() as conn:
        row = conn.execute(
            """SELECT COALESCE(SUM(cost_usd), 0) as total
            FROM llm_usage
            WHERE created_at >= date('now', 'start of month')"""
        ).fetchone()
        return row["total"]


def log_api_usage(source: str, endpoint: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO api_usage (source, endpoint) VALUES (?, ?)",
            (source, endpoint),
        )


def get_brand_stats_week() -> dict:
    with get_db() as conn:
        this_week = conn.execute(
            """SELECT
                COUNT(*) as total,
                COUNT(DISTINCT author) as unique_authors,
                SUM(CASE WHEN is_first_mention THEN 1 ELSE 0 END) as first_timers,
                SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
                SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral,
                SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative
            FROM brand_mentions
            WHERE ingested_at >= date('now', '-7 days')"""
        ).fetchone()
        last_week = conn.execute(
            """SELECT COUNT(*) as total
            FROM brand_mentions
            WHERE ingested_at >= date('now', '-14 days')
            AND ingested_at < date('now', '-7 days')"""
        ).fetchone()
        return {
            "this_week": dict(this_week),
            "last_week_total": last_week["total"],
        }


def get_filtered_opportunities_for_eval(limit: int = 10) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT o.*, s.text as item_text, s.source, s.url as item_url,
                      s.author as item_author
            FROM opportunities o
            JOIN seen_items s ON o.item_id = s.id
            WHERE o.score >= ?
            AND o.structural_observation IS NULL
            AND o.moment_type != 'skip'
            ORDER BY o.score DESC LIMIT ?""",
            (6, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def get_evaluated_for_generation(limit: int = 10) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT o.*, s.text as item_text, s.source, s.url as item_url,
                      s.author as item_author
            FROM opportunities o
            JOIN seen_items s ON o.item_id = s.id
            WHERE o.structural_observation IS NOT NULL
            AND (o.options_json IS NULL OR o.options_json = '[]')
            AND o.format_recommendation != 'no_post'
            ORDER BY o.score DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def update_opportunity_evaluation(opp_id: int, data: dict):
    with get_db() as conn:
        conn.execute(
            """UPDATE opportunities SET
                structural_observation = ?,
                pop_angle = ?,
                format_recommendation = ?,
                platform_recommendation = ?,
                timing_recommendation = ?,
                evaluation_rationale = ?
            WHERE id = ?""",
            (
                data.get("structural_observation", ""),
                data.get("pop_angle", ""),
                data.get("format_recommendation", ""),
                data.get("platform_recommendation", ""),
                data.get("timing_recommendation", ""),
                data.get("evaluation_rationale", ""),
                opp_id,
            ),
        )


def update_opportunity_options(opp_id: int, options: list):
    with get_db() as conn:
        conn.execute(
            "UPDATE opportunities SET options_json = ? WHERE id = ?",
            (json.dumps(options), opp_id),
        )
