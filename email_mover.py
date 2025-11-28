import functools
import json
import os
import random
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from multiprocessing import Manager, Pool

import pandas as pd
import requests
from clickhouse_driver import Client
from requests.exceptions import RequestException
from tqdm.notebook import tqdm


def load_env(path: str = ".env"):
    env_vars = {}

    try:
        with open(path, "r", encoding="utf-8") as env_file:
            for line in env_file:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue

                if "=" not in stripped:
                    continue

                key, value = stripped.split("=", 1)
                key = key.strip()
                value = value.strip()

                if (value.startswith("\"") and value.endswith("\"")) or (
                    value.startswith("'") and value.endswith("'")
                ):
                    value = value[1:-1]

                env_vars[key] = value
    except FileNotFoundError:
        pass

    return env_vars


env = {**load_env(), **os.environ}


def str_to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


api_key = env.get("EE_API", "xxxxxx")
service_url = env.get("EE_URL", "https://maileng.maildoso.co")
parallel_processes = int(env.get("PARALLEL_PROCESSES", 3))
logs_file = "logs.txt"

port_value = env.get("CH_PORT", "9000")
ch_port = int(port_value)
secure_default = ch_port in {443, 8443, 9440}

clickhouse_config = {
    "host": env.get("CH_HOST", "localhost"),
    "port": ch_port,
    "user": env.get("CH_USER", "default"),
    "password": env.get("CH_PASSWORD", ""),
    "database": env.get("CH_DATABASE", "default"),
    "secure": str_to_bool(env.get("CH_SECURE"), True),
    "verify": str_to_bool(env.get("CH_VERIFY"), False),
}

sql_query = """
SELECT ee_id, ee_account_id
FROM email_letters
WHERE 1=1
    AND date(created_at) >= today() - INTERVAL '1 day'
    AND path = '\\Junk'
    AND status = 'received'
"""


def retry_on_status_code(retries=3, delay=2):
    """
    A decorator to retry the function if 'statusCode' in the response JSON is not 200.

    :param retries: Number of retries before giving up.
    :param delay: Delay (in seconds) between retries.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            status_code = None
            while attempt < retries:
                response = func(*args, **kwargs)
                if not isinstance(response, dict):
                    raise Exception("The response is not a valid JSON or dictionary.")

                status_code = response.get("statusCode")
                if status_code == 200:
                    return response
                if status_code == 404:
                    return response

                attempt += 1
                print(f"Retry {attempt}/{retries} for {func.__name__} - Status: {status_code}")
                time.sleep(delay)

            print(f"Failed after {retries} retries. Last status: {status_code}")
            return response

        return wrapper

    return decorator


@retry_on_status_code(retries=5, delay=6)
def move_message_to_folder(account, message, folder="Junk Email"):
    # move message to a folder
    # folder should be either "SPAM" (for gmail) or "Junk Email" (for outlook)

    url = f"{service_url}/v1/account/{account}/message/{message}/move"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "x-ee-timeout": "6000",
    }

    params = {
        "path": folder,
    }

    response = requests.put(url, headers=headers, json=params)

    try:
        result = response.json()
        result["statusCode"] = response.status_code
    except requests.exceptions.JSONDecodeError:
        return {"statusCode": 500}

    return result


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


def fetch_messages(client):
    return client.execute(sql_query)


log_lock = None


def init_pool(lock):
    global log_lock
    log_lock = lock


def log_successful_id(message_id):
    # persist immediately to survive interruptions
    if log_lock:
        with log_lock:
            append_logged_ids(logs_file, [message_id])
    else:
        append_logged_ids(logs_file, [message_id])


def process_move(record):
    ee_id, ee_account_id = record
    response = move_message_to_folder(ee_account_id, ee_id, "INBOX")
    status_code = response.get("statusCode") if isinstance(response, dict) else None
    if status_code == 200:
        log_successful_id(ee_id)
        print(f"Moved message to INBOX: {ee_id}")
        return ee_id
    if status_code == 404:
        log_successful_id(ee_id)
        print(f"Message not found (404), recorded id to skip: {ee_id}")
        return None
    print(f"Failed to move message {ee_id}: status {status_code}")
    return None


def main():
    client = Client(**clickhouse_config)
    records = fetch_messages(client)

    processed_ids = read_logged_ids(logs_file)
    pending_records = [record for record in records if record[0] not in processed_ids]

    print(f"Fetched {len(pending_records)} messages from the database to move.")

    if not pending_records:
        print("No new messages to move.")
        return

    manager = Manager()
    lock = manager.Lock()

    with Pool(processes=parallel_processes, initializer=init_pool, initargs=(lock,)) as pool:
        results = pool.map(process_move, pending_records)

    successful_ids = [ee_id for ee_id in results if ee_id is not None]
    print(f"Moved {len(successful_ids)} messages to INBOX.")


if __name__ == "__main__":
    main()
