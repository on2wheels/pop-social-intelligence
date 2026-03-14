import json
import anthropic
from loguru import logger

import config
from db import (
    get_pending_items, mark_processed, insert_brand_mention,
    is_first_mention, log_llm_usage, get_daily_spend,
)

client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

def _is_false_positive(item: dict) -> bool:
    """Filter out known non-PoP entities that share similar names."""
    url = item.get("url", "").lower()
    text = item.get("text", "").lower()
    return any(
        domain.lower() in url or domain.lower() in text
        for domain in config.BRAND_EXCLUDE_DOMAINS
    )

BRAND_PROMPT = """You are a brand mention analyzer for PoP Network.

{mission_context}

Analyze each brand mention below. Determine sentiment, context, and whether a response is warranted.

ITEMS:
{items_block}

For EACH item, respond with a JSON object:
{{
    "item_id": <id>,
    "sentiment": "positive" | "neutral" | "negative" | "unknown",
    "context_type": "organic_discovery" | "referral" | "skeptical" | "hostile" | "press" | "influencer",
    "response_warranted": "yes" | "no" | "monitor",
    "suggested_response": "<1-2 sentence suggestion if response_warranted='yes', else null>",
    "reach_estimate": <integer>
}}

Context type guide:
- organic_discovery: someone found PoP on their own and is discussing it
- referral: someone recommending PoP to others
- skeptical: questioning PoP's approach or legitimacy (not hostile, just cautious)
- hostile: actively attacking or spreading misinformation about PoP
- press: media/journalist coverage
- influencer: high-follower account mentioning PoP

Response warranted guide:
- yes: positive engagement opportunity, correct misinformation, or thank influential mention
- no: routine neutral mention, no action needed
- monitor: watch for follow-up, don't engage yet

Return a JSON array. Nothing else."""


def _format_items(items: list[dict]) -> str:
    parts = []
    for item in items:
        first = is_first_mention(item.get("author", ""))
        parts.append(
            f"[ID: {item['id']}] Source: {item['source']}\n"
            f"Author: {item.get('author', 'unknown')}\n"
            f"Followers: {item.get('follower_count', 0)}\n"
            f"First mention from this author: {first}\n"
            f"Text: {item['text'][:600]}\n"
        )
    return "\n---\n".join(parts)


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens * 0.80 + output_tokens * 4.0) / 1_000_000


def process_pending():
    if get_daily_spend() >= config.DAILY_LLM_BUDGET_USD:
        logger.warning("Daily LLM budget exceeded, skipping brand analysis")
        return

    items = get_pending_items("brand", limit=50)
    if not items:
        logger.debug("No pending brand items to analyze")
        return

    before = len(items)
    items = [item for item in items if not _is_false_positive(item)]
    skipped = before - len(items)
    if skipped:
        logger.info(f"Filtered {skipped} false positive brand mentions")

    for i in range(0, len(items), 5):
        batch = items[i : i + 5]
        items_block = _format_items(batch)

        prompt = BRAND_PROMPT.format(
            mission_context=config.POP_MISSION_CONTEXT,
            items_block=items_block,
        )

        try:
            response = client.messages.create(
                model=config.HAIKU_MODEL,
                max_tokens=1500,
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
                "brand_analyze",
            )

            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            results = json.loads(text)

            for result in results:
                item_id = result["item_id"]
                # Find the matching item
                item = next((it for it in batch if it["id"] == item_id), None)
                if not item:
                    continue

                author = item.get("author", "unknown")
                first = is_first_mention(author)
                follower_count = item.get("follower_count", 0)

                # Override context_type for high-follower accounts
                context_type = result["context_type"]
                if follower_count > 10000 and context_type != "press":
                    context_type = "influencer"

                mention = {
                    "item_id": item_id,
                    "brand_term": _detect_brand_term(item.get("text", "")),
                    "mention_text": item.get("text", "")[:500],
                    "author": author,
                    "url": item.get("url", ""),
                    "sentiment": result["sentiment"],
                    "context_type": context_type,
                    "reach": follower_count or result.get("reach_estimate", 0),
                    "is_first_mention": first,
                    "response_warranted": result["response_warranted"],
                    "suggested_response": result.get("suggested_response"),
                }
                insert_brand_mention(mention)

            mark_processed([item["id"] for item in batch])
            logger.info(f"Brand analyzed batch of {len(batch)}")

        except json.JSONDecodeError as e:
            logger.error(f"Brand analyze JSON parse error: {e}")
            mark_processed([item["id"] for item in batch])
        except anthropic.APIError as e:
            logger.error(f"Anthropic API error in brand analyze: {e}")
        except Exception as e:
            logger.error(f"Brand analyze error: {e}")


def _detect_brand_term(text: str) -> str:
    text_lower = text.lower()
    for term in config.BRAND_TERMS:
        if term.lower() in text_lower:
            return term
    return "unknown"
