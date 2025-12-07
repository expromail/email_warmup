import random
import socket
from multiprocessing import Pool

from utils import (
    clickhouse_config_smtp_log,
    env,
    fetch_messages_http,
    send_email_via_connection,
    create_smtp_connection,
    smtp_port,
    smtp_server,
)

sql_query = """
SELECT sender_email, subject, email_body as plain_text, email_html
FROM
    smtp_logs
WHERE 1=1
    AND subject LIKE 'Simple fix for%deliverability'
    AND is_warmup = False
    AND is_followup = False
    AND is_spamtest = False
    AND date(ts) >= today() - INTERVAL '3 day'
    AND is_sent = True
"""

seed_list_file = "seed_list.txt"
warmup_senders_file = "email_to_warmup.txt"

same_sender_count = int(env.get("WARMUP_SAME_SENDER_COUNT", 1))
warmup_sender_count = int(env.get("WARMUP_WARMUP_SENDER_COUNT", 3))
parallel_processes = int(env.get("PARALLEL_PROCESSES", 3))


def read_lines(path):
    try:
        with open(path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        return []


def fetch_candidate_messages():
    records = fetch_messages_http(clickhouse_config_smtp_log, sql_query)
    return [
        {
            "sender": row.get("sender_email"),
            "subject": row.get("subject"),
            "plain_text": row.get("plain_text"),
            "html": row.get("email_html"),
        }
        for row in records
        if row.get("sender_email")
    ]


def build_body_parts(message):
    plain_text = message.get("plain_text")
    html = message.get("html")

    # Normalize empty strings to None
    plain_text = plain_text if plain_text else None
    html = html if html else None

    return plain_text, html


smtp_conn = None


def init_worker():
    global smtp_conn
    smtp_conn = create_smtp_connection()


def send_task(task):
    sender = task["sender"]
    recipient = task["recipient"]
    message = task["message"]

    plain_text, html = build_body_parts(message)
    subject = message.get("subject") or ""

    try:
        send_email_via_connection(
            smtp_conn,
            sender=sender,
            recipient=recipient,
            subject=subject,
            text=plain_text or "",
            html=html,
            message_type="warmup",
        )
        return True
    except Exception as exc:
        # Attempt one reconnect and retry
        try:
            new_conn = create_smtp_connection()
            globals()["smtp_conn"] = new_conn
            send_email_via_connection(
                new_conn,
                sender=sender,
                recipient=recipient,
                subject=subject,
                text=plain_text or "",
                html=html,
                message_type="warmup",
            )
            return True
        except Exception as inner_exc:
            print(f"Failed sending warmup from {sender} to {recipient}: {inner_exc}")
            return False


def main():
    if not smtp_server:
        print("SMTP_SERVER is not configured; aborting send.")
        return

    try:
        socket.getaddrinfo(smtp_server, smtp_port)
    except socket.gaierror as exc:
        print(f"SMTP server '{smtp_server}' cannot be resolved: {exc}. Aborting.")
        return

    seeds = read_lines(seed_list_file)
    warmup_senders = read_lines(warmup_senders_file)
    messages = fetch_candidate_messages()

    if not seeds:
        print("No recipients found in seed_list.txt")
        return

    if not warmup_senders:
        print("No warmup senders found in email_to_warmup.txt")
        return

    if not messages:
        print("No candidate messages fetched from smtp_logs.")
        return

    tasks = []
    for message in messages:
        sender = message["sender"]

        for _ in range(same_sender_count):
            tasks.append(
                {
                    "sender": sender,
                    "recipient": random.choice(seeds),
                    "message": message,
                }
            )

        for _ in range(warmup_sender_count):
            tasks.append(
                {
                    "sender": random.choice(warmup_senders),
                    "recipient": random.choice(seeds),
                    "message": message,
                }
            )

    with Pool(processes=parallel_processes, initializer=init_worker) as pool:
        results = pool.map(send_task, tasks)

    successes = sum(1 for r in results if r)
    print(f"Sent {successes} warmup emails (tasks={len(tasks)})")


if __name__ == "__main__":
    main()
