import argparse
import csv
import random
import socket
import time
from datetime import date
from multiprocessing import Pool

import psycopg2

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
FROM smtp_logs
WHERE
    is_warmup = false
    AND is_followup = false
    AND is_spamtest = false
    AND is_sent = true
    AND NOT match(message_headers, 'X-Ref-Id')
    AND NOT match(message_headers, 'X-Auto-Response-Suppress')
    AND NOT match(rcp_email, '@(gmail|yahoo|hotmail)\\.com$')
    AND ts >= today() - INTERVAL 3 DAY
ORDER BY rand()
LIMIT 5000;
"""

seed_list_file = "seed_list_all.csv"
same_sender_count = int(env.get("WARMUP_SAME_SENDER_COUNT", 6))
parallel_processes = int(env.get("PARALLEL_PROCESSES", 4))


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


def fetch_email_accounts(domain_parity):
    print("Fetching email accounts from PostgreSQL...")
    host = env.get("PG_HOST")
    port = env.get("PG_PORT", "5432")
    user = env.get("PG_USER")
    password = env.get("PG_PASSWORD")
    dbname = env.get("PG_DB")

    missing = [
        key
        for key, value in {
            "PG_HOST": host,
            "PG_USER": user,
            "PG_PASSWORD": password,
            "PG_DB": dbname,
        }.items()
        if not value
    ]
    if missing:
        print(f"Missing PostgreSQL config in .env: {', '.join(missing)}")
        return []

    query = """
SELECT
  ea.email_account,
  ea.domain_id
FROM email_accounts AS ea
JOIN email_domains  AS ed
  ON ed.id = ea.domain_id
WHERE
  ea.is_active = TRUE
  AND ea.is_hidden = FALSE
  AND ea.provider = 'MAILDOSO'
  AND ea.email_account NOT LIKE '%%maildoso%%'
  AND ea.domain_id %% 2 = %s         -- pass 0 for even, 1 for odd
  AND ed.domain_status <> 'BLACKLISTED';
    """

    try:
        with psycopg2.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            dbname=dbname,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (domain_parity,))
                rows = cur.fetchall()
                results = []
                skipped = 0
                for row in rows:
                    if len(row) < 2:
                        skipped += 1
                        continue
                    email_account = row[0]
                    domain_id = row[1]
                    if not email_account or domain_id is None:
                        skipped += 1
                        continue
                    results.append((email_account, domain_id))
                if skipped:
                    print(
                        "Warning: skipped "
                        f"{skipped} rows missing email_account or domain_id."
                    )
                return results
    except Exception as exc:
        print(f"Failed to fetch email accounts from PostgreSQL: {exc}")
        return []


def build_body_parts(message):
    plain_text = message.get("plain_text")
    html = message.get("html")

    plain_text = plain_text if plain_text else None
    html = html if html else None

    return plain_text, html


def pick_recipients(seed_pool, count):
    if not seed_pool:
        return []
    if count <= len(seed_pool):
        return random.sample(seed_pool, count)
    return random.choices(seed_pool, k=count)


smtp_conn = None
worker_tasks = []
worker_batches_processed = 0
worker_custom_message_id = None


def chunked_ranges(total, size):
    for start in range(0, total, size):
        end = min(start + size, total)
        yield (start, end)


def init_worker(tasks, custom_message_id):
    global smtp_conn, worker_tasks, worker_batches_processed, worker_custom_message_id
    worker_tasks = tasks
    worker_custom_message_id = custom_message_id
    smtp_conn = safe_create_smtp_connection()
    worker_batches_processed = 0


def safe_create_smtp_connection(max_attempts=3, base_delay=1.0):
    for attempt in range(1, max_attempts + 1):
        try:
            return create_smtp_connection()
        except Exception as exc:
            if attempt == max_attempts:
                print(f"Failed to create SMTP connection after {attempt} attempts: {exc}")
                return None
            delay = base_delay * (2 ** (attempt - 1))
            print(f"SMTP connection failed (attempt {attempt}): {exc}. Retrying in {delay:.1f}s.")
            time.sleep(delay)
    return None


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
            custom_message_id=worker_custom_message_id,
        )
        return True
    except Exception:
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
    batch_size = end - start
    if smtp_conn is None:
        smtp_conn = safe_create_smtp_connection()
        if smtp_conn is None:
            print(f"Skipping batch due to SMTP connection failure (batch size={batch_size})")
            return 0, batch_size

    for idx in range(start, end):
        task = worker_tasks[idx]
        if send_task(task):
            delivered += 1

    worker_batches_processed += 1
    if worker_batches_processed % 5 == 0:
        try:
            if smtp_conn is not None:
                smtp_conn.quit()
        except Exception:
            pass
        smtp_conn = safe_create_smtp_connection()
        if smtp_conn is None:
            print("SMTP connection refresh failed; continuing with next batch.")
    return delivered, batch_size


def main():
    parser = argparse.ArgumentParser(
        description="Send warmup emails across all mailboxes with domain grouping."
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
    print("Fetching candidate messages from database...")
    messages = fetch_candidate_messages()
    print(f"Finished fetching candidate messages (count={len(messages)})")

    if not seeds:
        print("No recipients found in seed_list_warmup.csv")
        return

    if not messages:
        print("No candidate messages fetched from smtp_logs.")
        return

    if same_sender_count <= 0:
        print("WARMUP_SAME_SENDER_COUNT must be greater than zero.")
        return

    today_parity = 0 if date.today().day % 2 == 0 else 1
    accounts = fetch_email_accounts(today_parity)
    if not accounts:
        parity_label = "even" if today_parity == 0 else "odd"
        print(f"No active email accounts found for {parity_label} domains.")
        return

    domain_accounts = {}
    for email_account, domain_id in accounts:
        domain_accounts.setdefault(domain_id, []).append(email_account)

    domain_ids = list(domain_accounts.keys())
    random.shuffle(domain_ids)
    print(
        "Fetched "
        f"{len(accounts)} email accounts across {len(domain_ids)} domains."
    )

    tasks = []
    for domain_id in domain_ids:
        accounts_for_domain = domain_accounts.get(domain_id, [])
        if not accounts_for_domain:
            continue
        domain_message = random.choice(messages)
        domain_recipients = pick_recipients(seeds, same_sender_count)
        for sender in accounts_for_domain:
            for recipient in domain_recipients:
                tasks.append(
                    {
                        "sender": sender,
                        "recipient": recipient,
                        "message": domain_message,
                    }
                )

    if not tasks:
        print("No warmup tasks were generated with current sender counts.")
        return

    total_sends = len(tasks)
    batches = list(chunked_ranges(total_sends, 100))

    with Pool(
        processes=args.parallel_processes,
        initializer=init_worker,
        initargs=(tasks, args.custom_message_id),
    ) as pool:
        successes = 0
        completed_tasks = 0
        for delivered, batch_size in pool.imap_unordered(
            process_batch, batches, chunksize=1
        ):
            successes += delivered
            completed_tasks += batch_size
            remaining = total_sends - completed_tasks
            print(
                "Batch finished: delivered "
                f"{delivered} warmup emails (batch size={batch_size}). "
                f"Remaining emails to send: {remaining}"
            )

    print(f"Sent {successes} warmup emails (tasks={total_sends}, batches={len(batches)})")


if __name__ == "__main__":
    main()
