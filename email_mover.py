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
    AND ee_account_id IN ( '00qky1lezp40912i',
'098yk4r2jkfs0hhn',
'0cz5e4agx45gbdy4',
'0oznweylbr6v4j8q',
'0qaxktvqc0qz193r',
'14ub99k21kmsnrsi',
'1c3f6tdnok3wkwvp',
'1gbqa0p9pdk63vbt',
'20dom942znv1nv7p',
'4cbdo0fjyrg3p51g',
'4q5koy03xw62vw5n',
'4vk71bm0zubtqqnn',
'5jkgb5xil4ocz0w8',
'5lisx3ivouk4d1sj',
'5nwhychvfiqtmzpx',
'66urowd0coqpynun',
'78jub9rkcfua1fyo',
'7dtv4b1esm8r610i',
'7sbxb48kgkdgntcw',
'7wdix4pk9r2ankf7',
'80mydegtwckgtg3q',
'8ju1lqtjqxjlqafd',
'8mubjheuixymoret',
'9a58vdbjm4ex0vx6',
'9a7oajxhd63sp2gs',
'9hvex4w2vhusk7zm',
'a0a18s1vapnm9wi8',
'a5x04zy7zy6v72de',
'afqjgmdzhx4j5rt1',
'arje5xv2pon39f9f',
'b9g171dm4keuqz3u',
'by2358xwyo9jghek',
'cyqqyt2tqh9sqepo',
'czspfnntplmubzu3',
'dbvulkki80lnvfu1',
'dmsmjvrz2yx8y65n',
'dvv48sar85x1bt0s',
'e28ev8pgdnkpv6kh',
'ee9yw7n5kzxhcfme',
'flfguep25hqynaog',
'fvvsz9yzs8o6jt0y',
'gip7bhhxy3rbvjrg',
'gx1qu6t8rtc41p7p',
'h6x7e3c3xf4x1hhh',
'h7nucb41f08064gz',
'hdhhxstvym59dwzz',
'hgzgeoqglayuo8k5',
'hkapt78qp6u2j38d',
'i4odh5il4o1naukw',
'icnncu13rb4nn8in',
'j8p51h6lao80xsk0',
'jkh82nax1ulj4yt9',
'jyt2zkvxs6vso02k',
'kaszweeqtmzswv3h',
'kuajo7aapkkvr2yy',
'kyqivsrbsabeo8bp',
'l0jlepitwp4w0cyk',
'lwtpz0yjp8ivxo2h',
'lxmhrdhdc4lu3j47',
'mm90foh6wwnmjpai',
'mw19r6dz5j4k29mq',
'o5lvfxg0vv137yry',
'oo5ciei2c29mh1lv',
'oz8mxi45i1wjiajx',
'p6375pqw0ltdsjno',
'p8mc5g70mvdlt689',
'pa50477n42dc59b7',
'pz0imb7a5ukhtg2f',
'pzcs4k3ehr4rdgvy',
'q221b3h9jdl8ksq2',
'q79dqjrddmgzutxa',
'qnp6xl6xfepm5htj',
'raiv4x7bhaivzpqy',
'rk4u60n1emkzo5hi',
'rsgtcat2ahwd3ryc',
'sy4ezmhtvy0lnw4x',
't4rnohk5dufry0jl',
'tlcl9pyh336v0hmf',
'tmn4zqjkhe0giyaz',
'uekb976smn6mm1br',
'ush8407qt989gzut',
'usj4w5sv6pi29q9p',
'v7jvpwgfgci3hi4h',
'vn2gyplej7jevjxq',
'voyj5s4ty9gaj3pj',
'whb7ldsvdmst9ab2',
'wva1zk9y85vvylik',
'x10t1ub0jwr3s3h6',
'x2xdx3ukzpo9gwcf',
'xeht9692kmqm2a5s',
'xnk8rmwji45pglbt',
'xp0e9df9fgggx3en',
'xt7uhus38u5lflrf',
'y5j6qr7gciov3w33',
'yamunpa98mff0hw4',
'ygxb1xndtv0cbhvf',
'z12bub8u6jdoptns',
'z7r4by56vytsbn25',
'zetg53ny0tiep7ir',
'zumiz35fgqs6wads' )
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
