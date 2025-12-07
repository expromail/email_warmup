from multiprocessing import Manager, Pool

from clickhouse_driver import Client
from utils import (
    clickhouse_config_spam_tests,
    env,
    fetch_messages,
    move_message_to_folder,
)

sql_query = """
SELECT ee_id, ee_account_id
FROM email_letters
WHERE 1=1
    AND date(created_at) >= today() - INTERVAL '2 day'
    AND path = '\\Inbox'
    AND status = 'received'
    AND id LIKE '%-smdz%'
"""

parallel_processes = int(env.get("PARALLEL_PROCESSES", 3))
logs_file = "spam_log.txt"


def read_logged_ids(path):
    try:
        with open(path, "r", encoding="utf-8") as log_file:
            return {line.strip() for line in log_file if line.strip()}
    except FileNotFoundError:
        return set()


def append_logged_ids(path, message_ids):
    if not message_ids:
        return

    with open(path, "a", encoding="utf-8") as log_file:
        for message_id in message_ids:
            log_file.write(f"{message_id}\n")


log_lock = None


def init_pool(lock):
    global log_lock
    log_lock = lock


def log_successful_id(message_id):
    if log_lock:
        with log_lock:
            append_logged_ids(logs_file, [message_id])
    else:
        append_logged_ids(logs_file, [message_id])


def process_move(record):
    ee_id, ee_account_id = record

    response = move_message_to_folder(ee_account_id, ee_id, "\Junk")
    status_code = response.get("statusCode") if isinstance(response, dict) else None
    if status_code == 200:
        log_successful_id(ee_id)
        print(f"Moved message to SPAM: {ee_id}")
        return ee_id
    if status_code == 404:
        print(f"Message not found (404) for account {ee_account_id}, tried all EE instances.")
        return None
    print(f"Failed to move message {ee_id}: status {status_code}")
    return None


def main():
    client = Client(**clickhouse_config_spam_tests)
    records = fetch_messages(client, sql_query)

    processed_ids = read_logged_ids(logs_file)
    pending_records = [record for record in records if record[0] not in processed_ids]

    print(f"Fetched {len(pending_records)} messages from the database to move to SPAM.")

    if not pending_records:
        print("No new messages to move.")
        return

    manager = Manager()
    lock = manager.Lock()

    with Pool(
        processes=parallel_processes,
        initializer=init_pool,
        initargs=(lock,),
    ) as pool:
        results = pool.map(process_move, pending_records)

    successful_ids = [ee_id for ee_id in results if ee_id is not None]
    print(f"Moved {len(successful_ids)} messages to SPAM.")


if __name__ == "__main__":
    main()
