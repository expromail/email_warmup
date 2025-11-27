import functools
import json
import random
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from multiprocessing import Pool

import pandas as pd
import requests
from clickhouse_driver import Client
from requests.exceptions import RequestException
from tqdm.notebook import tqdm

api_key = 'xxxxxx'
service_url = 'https://maileng.maildoso.co'
parallel_processes = 3
logs_file = "logs.txt"
clickhouse_config = {
    "host": "localhost",
    "port": 9000,
    "user": "default",
    "password": "",
    "database": "default",
}

sql_query = """
SELECT ee_id, ee_account_id
FROM email_letters
WHERE 1=1
    AND date(created_at) >= today() - INTERVAL '1 day'
    AND path = '\\Junk'
    AND ee_account_id IN ( '00qky1lezp40912i',
'098yk4r2jkfs0hhn',
'0cz5e4agx45gbdy4',
'0oznweylbr6v4j8q',
'0qaxktvqc0qz193r',
'14ub99k21kmsnrsi',
'1c3f6tdnok3wkwvp')
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


def process_move(record):
    ee_id, ee_account_id = record
    response = move_message_to_folder(ee_account_id, ee_id, "INBOX")
    status_code = response.get("statusCode") if isinstance(response, dict) else None
    return ee_id if status_code == 200 else None


def main():
    client = Client(**clickhouse_config)
    records = fetch_messages(client)

    processed_ids = read_logged_ids(logs_file)
    pending_records = [record for record in records if record[0] not in processed_ids]

    if not pending_records:
        print("No new messages to move.")
        return

    with Pool(processes=parallel_processes) as pool:
        results = pool.map(process_move, pending_records)

    successful_ids = [ee_id for ee_id in results if ee_id is not None]
    append_logged_ids(logs_file, successful_ids)
    print(f"Moved {len(successful_ids)} messages to INBOX.")


if __name__ == "__main__":
    main()
