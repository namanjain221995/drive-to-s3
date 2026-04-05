import os
import json
import uuid
import time
import logging
import boto3
import urllib.request
import urllib.parse
import urllib.error

from google.oauth2 import service_account
from google.auth.transport.requests import Request

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRET_NAME = os.environ["SECRET_NAME"]
WEBHOOK_URL = os.environ["WEBHOOK_URL"]
CHANNEL_TOKEN = os.environ["CHANNEL_TOKEN"]
START_PAGE_TOKEN_PARAM = os.environ["START_PAGE_TOKEN_PARAM"]
SHARED_DRIVE_ID = os.environ["SHARED_DRIVE_ID"]

SERVICE_ACCOUNT_SECRET_KEY = os.environ.get(
    "SERVICE_ACCOUNT_SECRET_KEY",
    "gdrive_service_account_json"
)

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"

sm = boto3.client("secretsmanager", region_name=AWS_REGION)
ssm = boto3.client("ssm", region_name=AWS_REGION)


def get_secret_dict():
    resp = sm.get_secret_value(SecretId=SECRET_NAME)
    secret_str = resp.get("SecretString")
    if not secret_str:
        raise RuntimeError(f"SecretString is empty for secret: {SECRET_NAME}")

    try:
        return json.loads(secret_str)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"SecretString is not valid JSON for secret: {SECRET_NAME}") from exc


def get_service_account_info(secret_dict):
    raw_value = secret_dict.get(SERVICE_ACCOUNT_SECRET_KEY)
    if raw_value is None:
        raise RuntimeError(
            f"Missing key '{SERVICE_ACCOUNT_SECRET_KEY}' in secret '{SECRET_NAME}'"
        )

    if isinstance(raw_value, dict):
        sa_info = raw_value
    elif isinstance(raw_value, str):
        try:
            sa_info = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Secret key '{SERVICE_ACCOUNT_SECRET_KEY}' does not contain valid JSON"
            ) from exc
    else:
        raise RuntimeError(
            f"Unsupported type for '{SERVICE_ACCOUNT_SECRET_KEY}': {type(raw_value).__name__}"
        )

    required_fields = ["type", "client_email", "private_key", "token_uri"]
    missing = [field for field in required_fields if not sa_info.get(field)]
    if missing:
        raise RuntimeError(
            f"Service account JSON is missing required fields: {', '.join(missing)}"
        )

    return sa_info


def get_google_access_token(sa_info):
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=[GOOGLE_DRIVE_SCOPE],
    )
    credentials.refresh(Request())

    if not credentials.token:
        raise RuntimeError("Failed to obtain Google access token from service account")

    return credentials.token


def http_get_json(url, bearer_token):
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {bearer_token}")
    req.add_header("Accept", "application/json")

    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def http_post_json(url, bearer_token, payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {bearer_token}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")

    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def save_start_page_token(token_value):
    ssm.put_parameter(
        Name=START_PAGE_TOKEN_PARAM,
        Value=token_value,
        Type="String",
        Overwrite=True
    )
    logger.info("Saved start page token to SSM: %s", START_PAGE_TOKEN_PARAM)


def get_start_page_token(access_token):
    url = (
        "https://www.googleapis.com/drive/v3/changes/startPageToken"
        f"?supportsAllDrives=true"
        f"&driveId={urllib.parse.quote(SHARED_DRIVE_ID)}"
    )

    resp = http_get_json(url, access_token)
    token = resp.get("startPageToken")
    if not token:
        raise RuntimeError(f"Google did not return startPageToken: {resp}")

    return token


def register_changes_watch(access_token, page_token):
    channel_id = str(uuid.uuid4())
    expiration_ms = int((time.time() + 6 * 24 * 3600) * 1000)

    url = (
        "https://www.googleapis.com/drive/v3/changes/watch"
        f"?pageToken={urllib.parse.quote(page_token)}"
        f"&supportsAllDrives=true"
        f"&includeItemsFromAllDrives=true"
        f"&restrictToMyDrive=false"
        f"&driveId={urllib.parse.quote(SHARED_DRIVE_ID)}"
    )

    payload = {
        "id": channel_id,
        "type": "web_hook",
        "address": WEBHOOK_URL,
        "token": CHANNEL_TOKEN,
        "expiration": expiration_ms
    }

    watch_resp = http_post_json(url, access_token, payload)
    return channel_id, watch_resp


def lambda_handler(event, context):
    logger.info("Starting Drive watch registration for Shared Drive: %s", SHARED_DRIVE_ID)

    try:
        secret_dict = get_secret_dict()
        sa_info = get_service_account_info(secret_dict)
        access_token = get_google_access_token(sa_info)

        start_page_token = get_start_page_token(access_token)
        save_start_page_token(start_page_token)

        channel_id, watch_resp = register_changes_watch(access_token, start_page_token)

        logger.info("Drive watch registered successfully. Channel ID: %s", channel_id)

        return {
            "ok": True,
            "mode": "shared_drive_service_account",
            "sharedDriveId": SHARED_DRIVE_ID,
            "startPageToken": start_page_token,
            "channelId": channel_id,
            "watchResponse": watch_resp
        }

    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        logger.exception("HTTPError while registering Drive watch")
        return {
            "ok": False,
            "errorType": "HTTPError",
            "status": exc.code,
            "reason": exc.reason,
            "body": error_body
        }

    except Exception as exc:
        logger.exception("Unhandled exception in lambda_handler")
        return {
            "ok": False,
            "errorType": type(exc).__name__,
            "error": str(exc)
        }