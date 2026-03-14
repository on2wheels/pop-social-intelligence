import json
import re
import anthropic
from loguru import logger

import config
from db import (
    get_evaluated_for_generation, update_opportunity_options,
    log_llm_usage, get_daily_spend,
)

client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

GENERATE_PROMPT = """You are a content generator for PoP Network's social media.

{mission_context}

Generate exactly 3 response options for this opportunity.

SOURCE: {source}
ORIGINAL TEXT:
{text}

EVALUATION:
- Structural observation: {structural_observation}
- PoP angle: {pop_angle}
- Format: {format_recommendation}
- Platform: {platform_recommendation}
- Timing: {timing_recommendation}

Generate three options as a JSON array:

OPTION A — Structural:
Pure signal, no emotional register. One factual observation in larger context.
If platform is X: max 280 characters. If Reddit: 2-3 paragraphs.
No CTA, no link. Just the observation.

--- Option B — Reply Hook ---
Write a reply hook under 200 characters total. It must be a factual reframe of the issue that ends with a short question. No hashtags, no links, no emojis. Designed to be posted as a reply inside an existing high-traffic thread — not as a standalone post.

OPTION C — Accountability Hook:
Connect representative behavior to constituent preference gap.
Include UTM-tagged link: {honeypot_url}?utm_source={platform}&utm_campaign={{topic_slug}}
Brief, pointed. Drives to honey pot.

Each option as JSON object:
{{
    "option": "A" | "B" | "C",
    "label": "Structural" | "Reply Hook" | "Hook",
    "draft_text": "<the post text>",
    "platform": "<x or reddit>",
    "format_type": "<reply, original_post, poll, etc>",
    "rationale": "<why this angle works>",
    "risk_notes": "<any risk concerns, or null>",
    "requires_manual_action": <bool>
}}

Return a JSON array of exactly 3 objects. Nothing else."""


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens * 3.0 + output_tokens * 15.0) / 1_000_000


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower().strip())
    return slug[:50].strip("-")


def process_pending():
    if get_daily_spend() >= config.DAILY_LLM_BUDGET_USD:
        logger.warning("Daily LLM budget exceeded, skipping generation")
        return

    items = get_evaluated_for_generation(limit=10)
    if not items:
        logger.debug("No evaluated opportunities to generate for")
        return

    for item in items:
        try:
            topic_slug = _slugify(item.get("structural_observation", "accountability"))

            prompt = GENERATE_PROMPT.format(
                mission_context=config.POP_MISSION_CONTEXT,
                source=item["source"],
                text=item["item_text"][:1500],
                structural_observation=item["structural_observation"],
                pop_angle=item["pop_angle"],
                format_recommendation=item["format_recommendation"],
                platform_recommendation=item["platform_recommendation"],
                timing_recommendation=item["timing_recommendation"],
                honeypot_url=config.HONEYPOT_URL,
                platform=item["platform_recommendation"],
                topic_slug=topic_slug,
            )

            response = client.messages.create(
                model=config.SONNET_MODEL,
                max_tokens=2000,
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
                "generate",
            )

            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            options = json.loads(text)
            if not isinstance(options, list) or len(options) != 3:
                logger.warning(f"Generate returned {len(options) if isinstance(options, list) else 'non-list'} options for opp {item['id']}")
                if isinstance(options, list) and len(options) > 0:
                    update_opportunity_options(item["id"], options)
                continue

            update_opportunity_options(item["id"], options)
            logger.info(f"Generated 3 options for opportunity {item['id']}")

        except json.JSONDecodeError as e:
            logger.error(f"Generate JSON parse error for opp {item['id']}: {e}")
        except anthropic.APIError as e:
            logger.error(f"Anthropic API error in generate: {e}")
        except Exception as e:
            logger.error(f"Generate error for opp {item['id']}: {e}")
