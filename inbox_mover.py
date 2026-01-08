import csv
import os
import time
import random
from multiprocessing import Manager, Pool
from clickhouse_driver import Client
from utils import (
    clickhouse_config_spam_tests,
    env,
    fetch_messages,
    move_message_to_folder,
)

sql_query = """
SELECT
    ee_id,
    arrayElement(recipients, 1) AS email
FROM email_letters_first_emails
WHERE 1=1
    AND date(created_at) >= today() - INTERVAL '1 day'
    AND path = '\\Junk'
    AND (id LIKE '<curious-gepard%' or id LIKE '%-mldz%')
"""

parallel_processes = int(env.get("PARALLEL_PROCESSES", 3))
logs_file = "inbox_log.csv"


def load_seed_accounts(path="seed_list_all.csv"):
    try:
        with open(path, newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            return {
                (row.get("email") or "").strip().lower(): (row.get("ee_account_id") or "").strip()
                for row in reader
                if (row.get("email") or "").strip() and (row.get("ee_account_id") or "").strip()
            }
    except FileNotFoundError:
        print(f"Seed list not found at {path}")
        return {}


def read_logged_ids(path):
    processed = set()
    try:
        with open(path, newline="", encoding="utf-8") as log_file:
            reader = csv.reader(log_file)
            for idx, row in enumerate(reader):
                if not row:
                    continue
                if idx == 0 and [col.strip().lower() for col in row] == ["ee_id", "ee_account_id"]:
                    continue
                if len(row) >= 2:
                    processed.add((row[0].strip(), row[1].strip()))
                elif len(row) == 1:
                    processed.add((row[0].strip(), ""))
    except FileNotFoundError:
        return set()

    return processed


def append_logged_ids(path, message_ids):
    if not message_ids:
        return

    file_exists = os.path.exists(path)

    with open(path, "a", newline="", encoding="utf-8") as log_file:
        writer = csv.writer(log_file)
        if not file_exists:
            writer.writerow(["ee_id", "ee_account_id"])
        for ee_id, ee_account_id in message_ids:
            writer.writerow([ee_id, ee_account_id])


log_lock = None


def init_pool(lock):
    global log_lock
    log_lock = lock


def log_successful_id(ee_id, ee_account_id):
    # persist immediately to survive interruptions
    if log_lock:
        with log_lock:
            append_logged_ids(logs_file, [(ee_id, ee_account_id)])
    else:
        append_logged_ids(logs_file, [(ee_id, ee_account_id)])


def process_move(record):
    ee_id, ee_account_id = record

    response = move_message_to_folder(ee_account_id, ee_id, "INBOX")
    status_code = response.get("statusCode") if isinstance(response, dict) else None
    if status_code == 200:
        log_successful_id(ee_id, ee_account_id)
        print(f"Moved message to INBOX: {ee_id} for account {ee_account_id}")
        return ee_id
    if status_code == 404:
#        print(f"Message not found (404) for account {ee_account_id}, tried all EE instances.")
        return None
#    print(f"Failed to move message {ee_id}: status {status_code}")
    return None


def main():
    client = Client(**clickhouse_config_spam_tests)
    records = fetch_messages(client, sql_query)

    seed_accounts = load_seed_accounts()
    if not seed_accounts:
        print("No seed accounts loaded; aborting.")
        return

    processed_pairs = read_logged_ids(logs_file)

    mapped_records = []
    missing_accounts = 0
    already_moved = 0

    for ee_id, email in records:
        account_id = seed_accounts.get((email or "").lower())
        if not account_id:
            missing_accounts += 1
            continue
        pair = (ee_id, account_id)
        if pair in processed_pairs:
            already_moved += 1
            continue
        mapped_records.append(pair)

    print(f"Fetched {len(records)} messages from the database to move.")
    if missing_accounts:
        print(f"Skipped {missing_accounts} messages without matching ee_account_id in seed_list.")
    if already_moved:
        print(f"Skipped {already_moved} messages already logged.")

    if not mapped_records:
        print("No new messages to move.")
        return

    random.shuffle(mapped_records)

    manager = Manager()
    lock = manager.Lock()

    with Pool(
        processes=parallel_processes,
        initializer=init_pool,
        initargs=(lock,),
    ) as pool:
        results = []
        total = len(mapped_records)
        for start in range(0, total, 600):
            batch = mapped_records[start : start + 600]
            results.extend(pool.map(process_move, batch))
            if start + 600 < total:
                print("Processed 600 messages, pausing for 7 seconds...")
                time.sleep(7)

    successful_ids = [ee_id for ee_id in results if ee_id is not None]
    print(f"Moved {len(successful_ids)} messages to INBOX.")


if __name__ == "__main__":
    main()
