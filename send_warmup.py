import csv
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
SELECT
  sender_email,
  subject,
  plain_text,
  email_html
FROM
(
  SELECT
    sender_email,
    subject,
    email_body AS plain_text,
    email_html
  FROM smtp_logs
  WHERE
    is_warmup = false
    AND is_followup = false
    AND is_spamtest = false
    AND is_sent = true
    AND email_html != email_body
    AND ts >= today() AND ts < today() + INTERVAL 1 DAY
    AND (cityHash64(sender_email, subject, ts) % 2) = 0
  LIMIT 50000          -- safety buffer
)
ORDER BY rand()
LIMIT 20000;
"""

seed_list_file = "seed_list.csv"
warmup_senders_file = "email_to_warmup.txt"

same_sender_count = int(env.get("WARMUP_SAME_SENDER_COUNT", 1))
warmup_sender_count = int(env.get("WARMUP_WARMUP_SENDER_COUNT", 3))
parallel_processes = int(env.get("PARALLEL_PROCESSES", 4))
send_from_original_sender = str(env.get("SEND_FROM_ORIGINAL_SENDER", "true")).lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def read_lines(path):
    try:
        with open(path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        return []


def read_seed_emails(path):
    try:
        with open(path, newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            return [
                (row.get("email") or "").strip()
                for row in reader
                if (row.get("email") or "").strip()
            ]
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
worker_messages = []
worker_seeds = []
worker_warmup_senders = []
worker_batches_processed = 0


def chunked_ranges(total, size):
    for start in range(0, total, size):
        end = min(start + size, total)
        yield (start, end)


def init_worker(messages, seeds, warmup_senders):
    global smtp_conn, worker_messages, worker_seeds, worker_warmup_senders, worker_batches_processed
    worker_messages = messages
    worker_seeds = seeds
    worker_warmup_senders = warmup_senders
    smtp_conn = create_smtp_connection()
    worker_batches_processed = 0


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


def process_batch(batch):
    global smtp_conn, worker_batches_processed
    start, end = batch
    delivered = 0
    sends_per_message = same_sender_count + warmup_sender_count
    for idx in range(start, end):
        message_index = idx // sends_per_message
        position = idx % sends_per_message
        message = worker_messages[message_index]

        primary_sender_pool = (
            [message["sender"]]
            if send_from_original_sender and message.get("sender")
            else worker_warmup_senders
        )

        if position < same_sender_count:
            sender = random.choice(primary_sender_pool)
        else:
            sender = random.choice(worker_warmup_senders)

        task = {
            "sender": sender,
            "recipient": random.choice(worker_seeds),
            "message": message,
        }
        if send_task(task):
            delivered += 1

    print(f"Delivered {delivered} warmup emails in this batch (batch size={end - start})")
    worker_batches_processed += 1
    if worker_batches_processed % 5 == 0:
        try:
            if smtp_conn is not None:
                smtp_conn.quit()
        except Exception:
            pass
        smtp_conn = create_smtp_connection()
    return delivered


def main():
    if not smtp_server:
        print("SMTP_SERVER is not configured; aborting send.")
        return

    try:
        socket.getaddrinfo(smtp_server, smtp_port)
    except socket.gaierror as exc:
        print(f"SMTP server '{smtp_server}' cannot be resolved: {exc}. Aborting.")
        return

    seeds = read_seed_emails(seed_list_file)
    warmup_senders = read_lines(warmup_senders_file)
    print("Fetching candidate messages from database...")
    messages = fetch_candidate_messages()
    print(f"Finished fetching candidate messages (count={len(messages)})")

    if not seeds:
        print("No recipients found in seed_list.csv")
        return

    if not warmup_senders:
        print("No warmup senders found in email_to_warmup.txt")
        return

    if not messages:
        print("No candidate messages fetched from smtp_logs.")
        return

    sends_per_message = same_sender_count + warmup_sender_count
    if sends_per_message <= 0:
        print("Warmup sender counts are zero; nothing to send.")
        return

    total_sends = len(messages) * sends_per_message
    batches = list(chunked_ranges(total_sends, 100))

    with Pool(
        processes=parallel_processes,
        initializer=init_worker,
        initargs=(messages, seeds, warmup_senders),
    ) as pool:
        results = pool.map(process_batch, batches, chunksize=1)

    successes = sum(results)
    print(f"Sent {successes} warmup emails (tasks={total_sends}, batches={len(batches)})")


if __name__ == "__main__":
    main()
