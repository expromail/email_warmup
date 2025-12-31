import csv
import random
import socket
from multiprocessing import Pool
from clickhouse_driver import Client

from utils import (
    clickhouse_config_smtp_log,
    clickhouse_config_spam_tests,
    env,
    fetch_messages_http,
    send_email_via_connection,
    create_smtp_connection,
    smtp_port,
    smtp_server,
)

messages_per_burned = 20

sql_messages = """
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
LIMIT 1000;
"""

sql_burned_senders = """
select sender as email
from reputation_test_results
where 1=1
AND date(created_at) = today() - INTERVAL 2 DAY
AND spam_google >= 3
AND sender NOT LIKE '%maildoso%'
AND sender NOT LIKE '%dosomail%'
"""

parallel_processes = int(env.get("PARALLEL_PROCESSES", 3))
seed_list_file = "seed_list.csv"


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


def fetch_messages_to_send():
    records = fetch_messages_http(clickhouse_config_smtp_log, sql_messages)
    return [
        {"subject": row.get("subject"), "plain_text": row.get("plain_text"), "html": row.get("html")}
        for row in records
    ]


def fetch_burned_senders():
    try:
        client = Client(**clickhouse_config_spam_tests)
        records = client.execute(sql_burned_senders)
        return [(row[0] or "").strip() for row in records if (row[0] or "").strip()]
    except Exception as exc:
        print(f"Failed to fetch burned senders via ClickHouse: {exc}")
        return []


def build_body_parts(message):
    plain_text = message.get("plain_text")
    html = message.get("html")

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
            )
            return True
        except Exception as exc:
            print(f"Failed sending warmup from burned sender {sender} to {recipient}: {exc}")
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

    seeds = read_seed_emails(seed_list_file)
    messages = fetch_messages_to_send()
    burned_senders = fetch_burned_senders()
    
    
    
    print(f"I will send {messages_per_burned} per {len(burned_senders)} burned mailboxes")

    if not seeds:
        print("No recipients found in seed_list.csv")
        return

    if not messages:
        print("No candidate messages fetched from smtp_logs.")
        return

    if not burned_senders:
        print("No burned senders found in reputation_test_results.")
        return

    tasks = []
    for sender in burned_senders:
        for _ in range(messages_per_burned):
            tasks.append(
                {
                    "sender": sender,
                    "recipient": random.choice(seeds),
                    "message": random.choice(messages),
                }
            )

    with Pool(processes=parallel_processes, initializer=init_worker) as pool:
        results = pool.map(send_task, tasks)

    successes = sum(1 for r in results if r)
    print(f"Sent {successes} emails from burned senders (tasks={len(tasks)})")


if __name__ == "__main__":
    main()
