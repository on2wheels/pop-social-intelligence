import json
import anthropic
from loguru import logger

import config
from db import (
    get_pending_items, mark_processed, insert_opportunity,
    get_recent_decisions, log_llm_usage, get_daily_spend,
)

client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

FILTER_PROMPT = """You are a political content filter for PoP Network.

{mission_context}

Your job: evaluate whether each item below is a genuine engagement opportunity
for PoP Network's non-partisan accountability platform.

RECENT DECISIONS (for novelty check):
{recent_decisions}

ITEMS TO EVALUATE:
{items_block}

For EACH item, respond with a JSON object in this exact format:
{{
    "item_id": <id>,
    "relevance_score": <0-10>,
    "moment_type": "reactive" | "structural" | "poll_worthy" | "skip",
    "risk_flags": {{
        "partisan": <bool>,
        "tone_deaf": <bool>,
        "legally_sensitive": <bool>
    }},
    "novelty_note": "<brief note if similar to recent content, else empty>"
}}

Scoring guide:
- 8-10: Perfect fit — structural accountability angle, high visibility, no partisan risk
- 6-7: Good fit — clear accountability angle but may need careful framing
- 4-5: Marginal — loosely related, high risk of seeming partisan
- 0-3: Skip — off-topic, too partisan, or already covered

Return a JSON array of objects. Nothing else."""


def _format_items(items: list[dict]) -> str:
    parts = []
    for item in items:
        parts.append(
            f"[ID: {item['id']}] Source: {item['source']}\n"
            f"Title: {item['title']}\n"
            f"Text: {item['text'][:500]}\n"
            f"Engagement: {item['engagement_count']}\n"
        )
    return "\n---\n".join(parts)


def _format_recent_decisions(decisions: list[dict]) -> str:
    if not decisions:
        return "No recent decisions yet."
    parts = []
    for d in decisions[:20]:
        parts.append(f"- [{d['action']}] {d['moment_type'] or 'n/a'} (score: {d['score'] or 'n/a'})")
    return "\n".join(parts)


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    # Haiku pricing: $0.80/M input, $4/M output
    return (input_tokens * 0.80 + output_tokens * 4.0) / 1_000_000


def process_pending():
    if get_daily_spend() >= config.DAILY_LLM_BUDGET_USD:
        logger.warning("Daily LLM budget exceeded, skipping filter")
        return

    items = get_pending_items("political", limit=50)
    if not items:
        logger.debug("No pending political items to filter")
        return

    recent = get_recent_decisions(20)
    recent_str = _format_recent_decisions(recent)

    # Process in batches of 10
    for i in range(0, len(items), 10):
        batch = items[i : i + 10]
        items_block = _format_items(batch)

        prompt = FILTER_PROMPT.format(
            mission_context=config.POP_MISSION_CONTEXT,
            recent_decisions=recent_str,
            items_block=items_block,
        )

        try:
            response = client.messages.create(
                model=config.HAIKU_MODEL,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )

            cost = _estimate_cost(
                response.usage.input_tokens, response.usage.output_tokens
            )
            log_llm_usage(
                config.HAIKU_MODEL,
                response.usage.input_tokens,
                response.usage.output_tokens,
                cost,
                "filter",
            )

            text = response.content[0].text.strip()
            # Handle markdown code blocks
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            results = json.loads(text)

            for result in results:
                item_id = result["item_id"]
                opp = {
                    "item_id": item_id,
                    "score": result["relevance_score"],
                    "moment_type": result["moment_type"],
                    "risk_flags": result["risk_flags"],
                }
                insert_opportunity(opp)

            mark_processed([item["id"] for item in batch])
            logger.info(
                f"Filtered batch of {len(batch)}: "
                f"{sum(1 for r in results if r['relevance_score'] >= config.FILTER_SCORE_THRESHOLD)} passed"
            )

        except json.JSONDecodeError as e:
            logger.error(f"Filter JSON parse error: {e}")
            # Mark processed anyway to avoid reprocessing bad items
            mark_processed([item["id"] for item in batch])
        except anthropic.APIError as e:
            logger.error(f"Anthropic API error in filter: {e}")
        except Exception as e:
            logger.error(f"Filter error: {e}")
