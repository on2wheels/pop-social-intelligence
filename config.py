import os
import sys
from dotenv import load_dotenv

load_dotenv()


def _require(var: str) -> str:
    val = os.getenv(var)
    if not val:
        print(f"ERROR: Missing required env var {var}. Copy .env.example to .env and fill it in.")
        sys.exit(1)
    return val


# --- API credentials ---
X_API_KEY = os.getenv("X_API_KEY", "")
X_API_SECRET = os.getenv("X_API_SECRET", "")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "")
X_ACCESS_SECRET = os.getenv("X_ACCESS_SECRET", "")
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "PoPNetwork/1.0")

ANTHROPIC_API_KEY = _require("ANTHROPIC_API_KEY")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")

HONEYPOT_URL = os.getenv("HONEYPOT_URL", "https://proofofpolitics.net/discover")

# --- Political content monitoring ---
KEYWORDS = [
    "congressional accountability", "term limits", "congressional reform",
    "judicial reform", "supreme court accountability", "foreign policy accountability",
    "immigration reform", "government transparency", "PAC reform",
    "representative voting record", "constituent vs representative",
    "civic engagement", "political collective action", "government accountability",
]

SUBREDDITS = [
    "politics", "PoliticalDiscussion", "Government", "TermLimits",
    "Corruption", "civics", "Democracy", "reform", "NeutralPolitics",
]

RSS_FEEDS = {
    "opensecrets_news": "https://www.opensecrets.org/news/feed/",
    "congress_gov_bills": "https://www.congress.gov/rss/most-viewed-bills.xml",
    "congress_gov_votes": "https://www.congress.gov/rss/house-floor-today.xml",
    "crew_ethics": "https://citizensforethics.org/news/feed/",
    "gao_blog": "https://www.gao.gov/blog/feed",
    "roll_call": "https://rollcall.com/feed/",
    "the_hill_congress": "https://thehill.com/homenews/house/feed/",
}

ACCOUNTS_TO_MONITOR = [
    "GovTrack", "OpenSecrets", "RepresentUs", "USTermLimits",
    "BrennanCenter", "FollowTheMoney",
]

# --- Brand monitoring ---
# Brand monitoring — specific to PoP Network only
# Deliberately excludes generic "popnet" / "pop network" to avoid
# Indonesian ISP (popnetofficial), Dutch academics (popnet.io),
# and Brazilian ISPs

BRAND_TERMS = [
    "@PoPNetHQ",
    "PoPNetHQ",
    "proofofpolitics.net",
    "proofofpolitics",
    "$POP token",
    "Proof of Politics Network",
    "PoP Network crypto",
    "PoP Network civic",
    "$1776 token",
    "$1776 governance",
]

BRAND_EXCLUDE_DOMAINS = [
    "popnet.io",                    # Dutch academic (Univ. of Amsterdam)
    "popnet.tv",                    # Content streaming site
    "popnet.id",                    # Indonesian ISP domain
    "popnetofficial",               # Indonesian ISP Instagram/social
    "popnet_pe",                    # Brazilian ISP Instagram
    "github.com/Zongwei97",         # Computer vision ML paper
    "ias.uva.nl",                   # Amsterdam academic institute
]

# ---- Excluded Topics ---------
EXCLUDE_TOPICS = [
    "january 6",
    "jan 6",
    "capitol riot",
    "insurrection",
]

# --- PoP mission context (injected into every LLM prompt) ---
POP_MISSION_CONTEXT = """
PoP Network (Proof of Politics) is a civic accountability platform that helps
citizens coordinate collective action to hold representatives accountable.
It is explicitly non-partisan — it does not take policy positions.
Its thesis: the gap between what constituents want and how representatives vote
is a structural problem requiring collective infrastructure, not better messaging.
The platform functions as a multi-sided marketplace connecting researchers,
broadcasters, and donors around issue-based communities with transparent
on-chain governance. The $1776 token represents foundational democratic
accountability principles. PoP never advocates for specific policy outcomes —
only for structural accountability and the right of constituents to be heard.
"""

# --- Generation options ---
# Option A: Structural (factual observation post)
# Option B: Reply Hook (short reply for high-traffic X threads, <200 chars)
# Option C: Accountability Hook (drives to honey pot with UTM link)

# --- LLM model assignments ---
HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6"

# --- Filtering thresholds ---
FILTER_SCORE_THRESHOLD = 6
MIN_ENGAGEMENT_POLITICAL = 50
MIN_ENGAGEMENT_BRAND = 0

# --- Cost controls ---
DAILY_LLM_BUDGET_USD = float(os.getenv("DAILY_LLM_BUDGET_USD", "3.00"))
MONTHLY_LLM_BUDGET_USD = float(os.getenv("MONTHLY_LLM_BUDGET_USD", "50.00"))

# --- Digest timing (hours in PT / America/Los_Angeles) ---
OPPORTUNITY_DIGEST_HOUR = 8
BRAND_DIGEST_HOUR = 9

# --- Database ---
DB_PATH = os.path.join(os.path.dirname(__file__), "social_intelligence.db")
