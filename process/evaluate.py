import json
import anthropic
from loguru import logger

import config
from db import (
    get_filtered_opportunities_for_eval, update_opportunity_evaluation,
    log_llm_usage, get_daily_spend,
)

client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

EVALUATE_PROMPT = """You are a strategic evaluator for PoP Network's social media presence.

{mission_context}

Evaluate this political content item as a potential engagement opportunity.

SOURCE: {source}
AUTHOR: {author}
TEXT:
{text}

FILTER OUTPUT:
- Score: {score}
- Moment type: {moment_type}
- Risk flags: {risk_flags}

Your job: determine the STRUCTURAL observation PoP can make. Not emotional, not opinionated.
Find the angle that only an accountability infrastructure platform would notice.

Respond with a single JSON object:
{{
    "structural_observation": "<the one factual point that reframes the conversation>",
    "pop_angle": "<the observation only an accountability platform would make — not left, not right>",
    "format_recommendation": "reply" | "original_post" | "poll" | "image_card" | "no_post",
    "platform_recommendation": "x" | "reddit" | "both",
    "timing_recommendation": "immediate" | "next_morning" | "next_relevant_moment",
    "evaluation_rationale": "<one sentence>"
}}

If the item has no good non-partisan angle, set format_recommendation to "no_post".
JSON only. Nothing else."""


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    # Sonnet pricing: $3/M input, $15/M output
    return (input_tokens * 3.0 + output_tokens * 15.0) / 1_000_000


def process_pending():
    if get_daily_spend() >= config.DAILY_LLM_BUDGET_USD:
        logger.warning("Daily LLM budget exceeded, skipping evaluation")
        return

    items = get_filtered_opportunities_for_eval(limit=10)
    if not items:
        logger.debug("No filtered opportunities to evaluate")
        return

    for item in items:
        try:
            prompt = EVALUATE_PROMPT.format(
                mission_context=config.POP_MISSION_CONTEXT,
                source=item["source"],
                author=item.get("item_author", "unknown"),
                text=item["item_text"][:1500],
                score=item["score"],
                moment_type=item["moment_type"],
                risk_flags=item.get("risk_flags", "{}"),
            )

            response = client.messages.create(
                model=config.SONNET_MODEL,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )

            cost = _estimate_cost(
                response.usage.input_tokens, response.usage.output_tokens
            )
            log_llm_usage(
                config.SONNET_MODEL,
                response.usage.input_tokens,
                response.usage.output_tokens,
                cost,
                "evaluate",
            )

            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            result = json.loads(text)
            update_opportunity_evaluation(item["id"], result)
            logger.info(
                f"Evaluated opportunity {item['id']}: "
                f"{result['format_recommendation']} on {result['platform_recommendation']}"
            )

        except json.JSONDecodeError as e:
            logger.error(f"Evaluate JSON parse error for opp {item['id']}: {e}")
        except anthropic.APIError as e:
            logger.error(f"Anthropic API error in evaluate: {e}")
        except Exception as e:
            logger.error(f"Evaluate error for opp {item['id']}: {e}")
