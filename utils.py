import functools
import os
import time

import requests

COMMON_CH_PORT_KEY = "CH_PORT"
COMMON_CH_SECURE_KEY = "CH_SECURE"
COMMON_CH_VERIFY_KEY = "CH_VERIFY"


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


def build_clickhouse_config(prefix: str):
    """
    Build a ClickHouse config for the given prefix, falling back to legacy
    CH_* keys so existing environments keep working.
    """

    port_value = env.get(COMMON_CH_PORT_KEY, "9000")
    ch_port = int(port_value)
    secure_default = ch_port in {443, 8443, 9440}

    def _get(key, fallback_key, default):
        return env.get(f"{prefix}_{key}", env.get(fallback_key, default))

    return {
        "host": _get("CH_HOST", "CH_HOST", "localhost"),
        "port": ch_port,
        "user": _get("CH_USER", "CH_USER", "default"),
        "password": _get("CH_PASSWORD", "CH_PASSWORD", ""),
        "database": _get("CH_DATABASE", "CH_DATABASE", "default"),
        "secure": str_to_bool(env.get(COMMON_CH_SECURE_KEY), True if secure_default else False),
        "verify": str_to_bool(env.get(COMMON_CH_VERIFY_KEY), False),
    }


clickhouse_config_spam_tests = build_clickhouse_config("SPAM_TESTS")
clickhouse_config_smtp_log = build_clickhouse_config("SMTP_LOG")


def retry_on_status_code(retries=3, delay=2, stop_statuses=None):
    """
    Retry the function if 'statusCode' in the response JSON is not 200.
    """
    stop_statuses = set(stop_statuses) if stop_statuses is not None else {200}

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
                if status_code in stop_statuses:
                    return response

                attempt += 1
                print(f"Retry {attempt}/{retries} for {func.__name__} - Status: {status_code}")
                time.sleep(delay)

            print(f"Failed after {retries} retries. Last status: {status_code}")
            return response

        return wrapper

    return decorator


def _ee_services():
    return [
        {
            "url": env.get("EE_PRIMARY_URL", "https://maileng.maildoso.co"),
            "api_key": env.get("EE_PRIMARY_API", ""),
        },
        {
            # fall back to legacy EE_URL/EE_API values for backward compatibility
            "url": env.get("EE_SECONDARY_URL", env.get("EE_URL", "https://maileng-2.maildoso.co")),
            "api_key": env.get("EE_SECONDARY_API", env.get("EE_API", "")),
        },
    ]


@retry_on_status_code(retries=5, delay=6, stop_statuses={200, 404})
def _move_message_with_service(service_url, api_key, account, message, folder="Junk Email"):
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
    except requests.exceptions.JSONDecodeError:
        result = {}

    result["statusCode"] = response.status_code
    result["serviceUrlTried"] = service_url
    return result


def move_message_to_folder(account, message, folder="Junk Email"):
    services = [svc for svc in _ee_services() if svc.get("url")]
    last_response = {"statusCode": None}

    for idx, service in enumerate(services):
        if not service.get("api_key"):
            print(f"Skipping EE instance {service['url']} due to missing API key.")
            continue

        last_response = _move_message_with_service(
            service["url"], service["api_key"], account, message, folder
        )

        status_code = last_response.get("statusCode")
        if status_code == 404 and idx + 1 < len(services):
            print(f"404 from {service['url']} for message {message}, trying next instance.")
            continue

        return last_response

    return last_response


def fetch_messages(client, query):
    return client.execute(query)
