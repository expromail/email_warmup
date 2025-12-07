import random
from multiprocessing import Pool

import socket

from utils import (
    clickhouse_config_smtp_log,
    env,
    fetch_messages_http,
    send_email,
    smtp_server,
    smtp_port,
)

sql_query = """
SELECT
    subject as subject,
    email_body as plain_text,
    email_html as html
FROM smtp_logs
WHERE 1=1
    AND is_warmup = 0
    AND is_followup = 0
    AND is_spamtest = 0
    AND date(ts) >= today() - INTERVAL 2 day
    AND is_sent = 1
    AND rand() % 1000 = 0
LIMIT 10;
"""

parallel_processes = int(env.get("PARALLEL_PROCESSES", 3))
spam_senders_file = "spam_senders.txt"
seed_list_file = "seed_list.txt"


def read_lines(path):
    try:
        with open(path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        return []


def fetch_candidate_messages():
    records = fetch_messages_http(clickhouse_config_smtp_log, sql_query)
    return [
        {"subject": row.get("subject"), "plain_text": row.get("plain_text"), "html": row.get("html")}
        for row in records
    ]


def build_body_parts(message):
    plain_text = message.get("plain_text")
    html = message.get("html")

    # Normalize empty strings to None
    plain_text = plain_text if plain_text else None
    html = html if html else None

    return plain_text, html


def send_from_sender(args):
    sender, messages, seeds = args
    if not messages:
        print(f"No messages to send for sender {sender}")
        return 0

    message = random.choice(messages)
    plain_text, html = build_body_parts(message)
    subject = message.get("subject") or ""

    if not seeds:
        print(f"No seed recipients available for sender {sender}")
        return 0

    recipients = random.sample(list(seeds), min(5, len(seeds)))
    recipients_str = ",".join(recipients)

    try:
        send_email(
            sender=sender,
            recipients=recipients_str,
            subject=subject,
            text=plain_text or "",
            html=html,
            message_type="spam",
        )
        print(f"Sent spam email from {sender} to {len(recipients)} recipients.")
        return len(recipients)
    except Exception as exc:
        print(f"Failed sending from {sender} to seeds {recipients}: {exc}")
        return 0


def main():
    if not smtp_server:
        print("SMTP_SERVER is not configured; aborting send.")
        return

    try:
        socket.getaddrinfo(smtp_server, smtp_port)
    except socket.gaierror as exc:
        print(f"SMTP server '{smtp_server}' cannot be resolved: {exc}. Aborting.")
        return

    senders = read_lines(spam_senders_file)
    seeds = read_lines(seed_list_file)
    messages = fetch_candidate_messages()

    if not senders:
        print("No senders found in spam_senders.txt")
        return

    if not seeds:
        print("No recipients found in seed_list.txt")
        return

    if not messages:
        print("No candidate messages fetched from smtp_logs.")
        return

    with Pool(processes=parallel_processes) as pool:
        tasks = [(sender, messages, seeds) for sender in senders]
        results = pool.map(send_from_sender, tasks)

    total_sent = sum(results)
    print(f"Completed sending. Total recipient deliveries: {total_sent}")


if __name__ == "__main__":
    main()
