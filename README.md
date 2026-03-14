# PoP Network — Social Intelligence Monitor

A read-only civic accountability monitoring tool that tracks public political discourse to surface conversations where non-partisan data on representative voting records vs. constituent polling adds useful context.

## What it does

- Monitors public political subreddits and RSS feeds for relevant discussions
- Scores content by relevance to civic accountability topics
- Surfaces opportunities for data-backed, non-partisan engagement
- Requires human editorial review before any action is taken

## What it does NOT do

- No automated posting, commenting, voting, or messaging
- No long-term data retention (items discarded within 48 hours)
- No user profiling or PII collection
- No AI training on Reddit data

## Stack

- Python 3.11 / PRAW
- SQLite (local, ephemeral)
- Anthropic API (content scoring)
- Telegram (human review interface)

## Reddit API Usage

- Read-only access to public subreddits
- ~10 subreddits polled 2–4x/day
- Well within free tier rate limits
- User agent: `PoPNetwork/1.0 by /u/0n2wheels`

## Platform

[Proof of Politics Network](https://proofofpolitics.net) — civic accountability infrastructure.
