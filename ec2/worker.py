import os
import json
import time
import logging
import urllib.request
import urllib.error

import boto3
from botocore.exceptions import ClientError
from google.oauth2 import service_account
from google.auth.transport.requests import Request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SECRET_NAME = os.environ["SECRET_NAME"]
SERVICE_ACCOUNT_SECRET_KEY = os.environ.get("SERVICE_ACCOUNT_SECRET_KEY", "gdrive_service_account_json")

ARCHIVE_QUEUE_URL = os.environ["ARCHIVE_QUEUE_URL"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")
INSTANCE_ID = os.environ["INSTANCE_ID"]

IDLE_STOP_MINUTES = int(os.environ.get("IDLE_STOP_MINUTES", "10"))
SQS_WAIT_SECONDS = int(os.environ.get("SQS_WAIT_SECONDS", "20"))
VISIBILITY_TIMEOUT = int(os.environ.get("VISIBILITY_TIMEOUT", "3600"))
PART_SIZE_MB = int(os.environ.get("PART_SIZE_MB", "16"))
PART_SIZE_BYTES = PART_SIZE_MB * 1024 * 1024

GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"

sm = boto3.client("secretsmanager", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)
ec2 = boto3.client("ec2", region_name=AWS_REGION)


def get_secret_dict():
    resp = sm.get_secret_value(SecretId=SECRET_NAME)
    secret_str = resp.get("SecretString")
    if not secret_str:
        raise RuntimeError(f"SecretString empty for secret: {SECRET_NAME}")
    return json.loads(secret_str)


def get_service_account_info(secret_dict):
    raw_value = secret_dict.get(SERVICE_ACCOUNT_SECRET_KEY)
    if raw_value is None:
        raise RuntimeError(
            f"Missing key '{SERVICE_ACCOUNT_SECRET_KEY}' in secret '{SECRET_NAME}'"
        )

    if isinstance(raw_value, dict):
        sa_info = raw_value
    elif isinstance(raw_value, str):
        sa_info = json.loads(raw_value)
    else:
        raise RuntimeError(
            f"Unsupported type for '{SERVICE_ACCOUNT_SECRET_KEY}': {type(raw_value).__name__}"
        )

    required_fields = ["type", "client_email", "private_key", "token_uri"]
    missing = [field for field in required_fields if not sa_info.get(field)]
    if missing:
        raise RuntimeError(f"Service account JSON missing fields: {', '.join(missing)}")

    return sa_info


def get_google_access_token():
    secret_dict = get_secret_dict()
    sa_info = get_service_account_info(secret_dict)

    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=[GOOGLE_DRIVE_SCOPE],
    )
    credentials.refresh(Request())

    if not credentials.token:
        raise RuntimeError("Failed to get Google access token")

    return credentials.token


def http_get_json(url, bearer_token):
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {bearer_token}")
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {e.code} GET {url} :: {body}")


def get_drive_file_meta(file_id, access_token):
    url = (
        f"https://www.googleapis.com/drive/v3/files/{file_id}"
        "?supportsAllDrives=true"
        "&fields=id,name,mimeType,size,md5Checksum"
    )
    return http_get_json(url, access_token)


def join_s3_key(*parts):
    cleaned = []
    for p in parts:
        if p is None:
            continue
        p = str(p).strip("/")
        if p:
            cleaned.append(p)
    return "/".join(cleaned)


def build_s3_key(job):
    return join_s3_key(
        S3_PREFIX,
        job["rootFolder"],
        job["slotFolder"],
        job["candidateFolder"],
        job["deliverableFolder"],
        job["finalFileName"],
    )


def s3_object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NotFound", "NoSuchKey"}:
            return False
        raise


def stream_drive_file_to_s3_multipart(file_id, job, access_token):
    file_meta = get_drive_file_meta(file_id, access_token)
    mime_type = file_meta.get("mimeType") or "application/octet-stream"

    if mime_type.startswith("application/vnd.google-apps."):
        raise RuntimeError(f"Google native file not supported for archive: {mime_type}")

    s3_key = build_s3_key(job)

    create_resp = s3.create_multipart_upload(
        Bucket=S3_BUCKET,
        Key=s3_key,
        ContentType=mime_type,
        Metadata={
            "drive-file-id": file_id,
            "candidate-folder": job["candidateFolder"],
            "deliverable-folder": job["deliverableFolder"],
            "slot-folder": job["slotFolder"],
        }
    )
    upload_id = create_resp["UploadId"]
    parts = []

    url = (
        f"https://www.googleapis.com/drive/v3/files/{file_id}"
        "?alt=media"
        "&supportsAllDrives=true"
    )
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {access_token}")

    part_number = 1

    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            while True:
                chunk = resp.read(PART_SIZE_BYTES)
                if not chunk:
                    break

                upload_part_resp = s3.upload_part(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk
                )

                parts.append({
                    "PartNumber": part_number,
                    "ETag": upload_part_resp["ETag"]
                })
                logger.info("Uploaded part %s for %s", part_number, s3_key)
                part_number += 1

        if not parts:
            empty_part = s3.upload_part(
                Bucket=S3_BUCKET,
                Key=s3_key,
                UploadId=upload_id,
                PartNumber=1,
                Body=b""
            )
            parts.append({
                "PartNumber": 1,
                "ETag": empty_part["ETag"]
            })

        s3.complete_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )

        logger.info("Completed upload to s3://%s/%s", S3_BUCKET, s3_key)
        return s3_key

    except Exception:
        logger.exception("Multipart upload failed. Aborting upload_id=%s", upload_id)
        try:
            s3.abort_multipart_upload(
                Bucket=S3_BUCKET,
                Key=s3_key,
                UploadId=upload_id
            )
        except Exception:
            logger.exception("Failed to abort multipart upload")
        raise


def stop_self():
    logger.info("Stopping EC2 instance: %s", INSTANCE_ID)
    ec2.stop_instances(InstanceIds=[INSTANCE_ID])


def process_message(msg):
    body = json.loads(msg["Body"])
    logger.info("Processing job: %s", json.dumps(body))

    required = [
        "fileId",
        "rootFolder",
        "slotFolder",
        "candidateFolder",
        "deliverableFolder",
        "finalFileName",
    ]
    missing = [k for k in required if not body.get(k)]
    if missing:
        raise RuntimeError(f"Missing required job fields: {', '.join(missing)}")

    s3_key = build_s3_key(body)

    if s3_object_exists(S3_BUCKET, s3_key):
        logger.info(
            "Skipping duplicate archive because object already exists: s3://%s/%s",
            S3_BUCKET,
            s3_key,
        )
        sqs.delete_message(
            QueueUrl=ARCHIVE_QUEUE_URL,
            ReceiptHandle=msg["ReceiptHandle"]
        )
        return

    access_token = get_google_access_token()
    uploaded_s3_key = stream_drive_file_to_s3_multipart(
        file_id=body["fileId"],
        job=body,
        access_token=access_token
    )

    sqs.delete_message(
        QueueUrl=ARCHIVE_QUEUE_URL,
        ReceiptHandle=msg["ReceiptHandle"]
    )
    logger.info("Deleted SQS message after success. s3_key=%s", uploaded_s3_key)


def main():
    logger.info("Worker started. Queue=%s Bucket=%s", ARCHIVE_QUEUE_URL, S3_BUCKET)
    last_work_time = time.time()

    while True:
        resp = sqs.receive_message(
            QueueUrl=ARCHIVE_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=SQS_WAIT_SECONDS,
            VisibilityTimeout=VISIBILITY_TIMEOUT
        )

        messages = resp.get("Messages", [])
        if not messages:
            idle_seconds = time.time() - last_work_time
            if idle_seconds >= IDLE_STOP_MINUTES * 60:
                logger.info("Idle timeout reached (%s minutes). Stopping self.", IDLE_STOP_MINUTES)
                stop_self()
                break
            continue

        for msg in messages:
            try:
                process_message(msg)
                last_work_time = time.time()
            except Exception:
                logger.exception("Message processing failed. It will return to the queue.")
                try:
                    sqs.change_message_visibility(
                        QueueUrl=ARCHIVE_QUEUE_URL,
                        ReceiptHandle=msg["ReceiptHandle"],
                        VisibilityTimeout=0
                    )
                except Exception:
                    logger.exception("Failed to reset message visibility")

        time.sleep(1)


if __name__ == "__main__":
    main()