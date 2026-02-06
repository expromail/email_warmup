import argparse
import csv
import random
import socket
import time
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
  original_sender_email,
  subject,
  email_body AS plain_text,
  email_html
FROM enriched_smtp_logs
WHERE 1=1
  AND user_id = 1711
  AND is_warmup = False
  AND is_spamtest = False
  AND is_followup = False
  AND is_sent = True
  AND date(ts) >= today() - INTERVAL '3 day'
ORDER BY rand()
LIMIT 10
"""

sql_query222 = '''
SELECT
  sender_email,
  original_sender_email,
  subject,
  email_body AS plain_text,
  email_html
FROM smtp_logs

WHERE 1=1
    AND email_html LIKE '%6ab3d7176947b45d8%'
    AND date(ts) >= yesterday()
LIMIT 10
'''


seed_list_file = "seed_list_warmup.csv"
warmup_senders_file = "email_to_warmup.txt"

parallel_processes = int(env.get("PARALLEL_PROCESSES", 4))


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
            "original_sender": row.get("original_sender_email"),
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
worker_tasks = []
worker_seeds = []
worker_batches_processed = 0
worker_custom_message_id = None
worker_references = False
worker_reply_to_sender = False


def create_smtp_connection_with_retry(max_attempts=3, delay=3):
    for attempt in range(1, max_attempts + 1):
        try:
            return create_smtp_connection()
        except Exception as exc:
            print(f"SMTP connection failed (attempt {attempt}/{max_attempts}): {exc}")
            if attempt < max_attempts:
                time.sleep(delay)
    return None


def chunked_ranges(total, size):
    for start in range(0, total, size):
        end = min(start + size, total)
        yield (start, end)


def init_worker(tasks, seeds, custom_message_id, references, reply_to_sender):
    global smtp_conn, worker_tasks, worker_seeds, worker_batches_processed, worker_custom_message_id
    global worker_references, worker_reply_to_sender
    worker_tasks = tasks
    worker_seeds = seeds
    smtp_conn = create_smtp_connection_with_retry()
    worker_batches_processed = 0
    worker_custom_message_id = custom_message_id
    worker_references = references
    worker_reply_to_sender = reply_to_sender


def send_task(task):
    sender = task["sender"]
    recipient = task["recipient"]
    message = task["message"]

    global smtp_conn, worker_custom_message_id, worker_references, worker_reply_to_sender
    if smtp_conn is None:
        smtp_conn = create_smtp_connection_with_retry()
        if smtp_conn is None:
            print(f"Skipping send from {sender} to {recipient}: no SMTP connection.")
            return False

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
            references=worker_references,
            reply_to=sender if worker_reply_to_sender else None,
            custom_message_id=worker_custom_message_id,
        )
        return True
    except Exception as exc:
        # Attempt one reconnect and retry
        try:
            new_conn = create_smtp_connection_with_retry()
            if new_conn is None:
                raise Exception("SMTP reconnect failed")
            globals()["smtp_conn"] = new_conn
            send_email_via_connection(
                new_conn,
                sender=sender,
                recipient=recipient,
                subject=subject,
                text=plain_text or "",
                html=html,
                message_type="warmup",
                references=worker_references,
                reply_to=sender if worker_reply_to_sender else None,
                custom_message_id=worker_custom_message_id,
            )
            return True
        except Exception as inner_exc:
            print(f"Failed sending warmup from {sender} to {recipient}: {inner_exc}")
            return False


def process_batch(batch):
    global smtp_conn, worker_batches_processed
    start, end = batch
    delivered = 0
    for idx in range(start, end):
        task = worker_tasks[idx]
        task = {
            "sender": task["sender"],
            "recipient": random.choice(worker_seeds),
            "message": task["message"],
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
        smtp_conn = create_smtp_connection_with_retry()
    return delivered, (end - start)


def main():
    parser = argparse.ArgumentParser(
        description="Send warmup emails from each warmup sender."
    )
    parser.add_argument(
        "parallel_processes",
        nargs="?",
        type=int,
        default=parallel_processes,
        help="Number of parallel worker processes (overrides PARALLEL_PROCESSES).",
    )
    parser.add_argument(
        "-m",
        "--message-id-tag",
        dest="custom_message_id",
        default=None,
        help="Custom tag to include in Message-ID local part (before UUID).",
    )
    parser.add_argument(
        "-r",
        "--references",
        action="store_true",
        help="Add References header identical to Message-ID.",
    )
    parser.add_argument(
        "-rt",
        "--reply-to",
        dest="reply_to_sender",
        action="store_true",
        help="Add Reply-To header identical to the sender email.",
    )
    args = parser.parse_args()
    if args.parallel_processes <= 0:
        print("parallel_processes must be a positive integer.")
        return

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

    tasks = []
    for message in messages:
        for sender in warmup_senders:
            tasks.append({"sender": sender, "message": message})

    if not tasks:
        print("No warmup tasks were generated.")
        return

    total_sends = len(tasks)
    batches = list(chunked_ranges(total_sends, 100))

    with Pool(
        processes=args.parallel_processes,
        initializer=init_worker,
        initargs=(
            tasks,
            seeds,
            args.custom_message_id,
            args.references,
            args.reply_to_sender,
        ),
    ) as pool:
        completed_sends = 0
        successes = 0
        for delivered, batch_size in pool.imap_unordered(
            process_batch, batches, chunksize=1
        ):
            completed_sends += batch_size
            successes += delivered
            remaining = total_sends - completed_sends
            print(
                f"Progress: sent={successes}, completed={completed_sends}, remaining={remaining}"
            )

    print(f"Sent {successes} warmup emails (tasks={total_sends}, batches={len(batches)})")


if __name__ == "__main__":
    main()
