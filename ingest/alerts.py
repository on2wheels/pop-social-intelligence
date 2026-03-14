import imaplib
import email
from email.header import decode_header
import re
from datetime import datetime, timezone
from loguru import logger

import config
from db import insert_item


def _decode_subject(msg) -> str:
    subject = msg.get("Subject", "")
    decoded_parts = decode_header(subject)
    parts = []
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            parts.append(part.decode(encoding or "utf-8", errors="replace"))
        else:
            parts.append(part)
    return " ".join(parts)


def _extract_body(msg) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="replace")
                    break
            elif part.get_content_type() == "text/html" and not body:
                payload = part.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            body = payload.decode("utf-8", errors="replace")
    return body


def _extract_urls(text: str) -> list[str]:
    return re.findall(r'https?://[^\s<>"\')\]]+', text)


def ingest():
    if not config.GMAIL_ADDRESS or not config.GMAIL_APP_PASSWORD:
        logger.warning("Gmail credentials not configured, skipping alerts ingest")
        return

    inserted = 0

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(config.GMAIL_ADDRESS, config.GMAIL_APP_PASSWORD)
        mail.select("inbox")

        _, message_ids = mail.search(
            None, '(FROM "alerts-noreply@google.com" UNSEEN)'
        )

        if not message_ids[0]:
            logger.debug("No new Google Alerts")
            mail.logout()
            return

        for msg_id in message_ids[0].split():
            try:
                _, msg_data = mail.fetch(msg_id, "(RFC822)")
                msg = email.message_from_bytes(msg_data[0][1])

                subject = _decode_subject(msg)
                body = _extract_body(msg)
                urls = _extract_urls(body)

                is_brand = subject.startswith("[BRAND]")
                item_type = "brand" if is_brand else "political"
                clean_subject = subject.replace("[BRAND]", "").strip() if is_brand else subject

                external_id = f"gmail_{msg.get('Message-ID', msg_id.decode())}"

                item = {
                    "source": "gmail",
                    "external_id": external_id[:200],
                    "text": body[:2000],
                    "title": clean_subject[:200],
                    "url": urls[0] if urls else "",
                    "author": "Google Alerts",
                    "engagement_count": 0,
                    "follower_count": 0,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "item_type": item_type,
                    "raw": {"subject": subject, "urls": urls[:5]},
                }

                if insert_item(item):
                    inserted += 1

                # Mark as read
                mail.store(msg_id, "+FLAGS", "\\Seen")

            except Exception as e:
                logger.error(f"Error processing alert email {msg_id}: {e}")

        mail.logout()

    except imaplib.IMAP4.error as e:
        logger.error(f"Gmail IMAP error: {e}")
    except Exception as e:
        logger.error(f"Gmail alerts error: {e}")

    logger.info(f"Gmail alerts ingest complete: {inserted} new items")
