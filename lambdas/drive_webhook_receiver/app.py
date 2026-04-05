import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
QUEUE_URL = os.environ["QUEUE_URL"]
EXPECTED_CHANNEL_TOKEN = os.environ["EXPECTED_CHANNEL_TOKEN"]

sqs = boto3.client("sqs", region_name=AWS_REGION)


def _lower_headers(headers: dict) -> dict:
    out = {}
    for k, v in (headers or {}).items():
        out[str(k).lower()] = v
    return out


def lambda_handler(event, context):
    logger.info("Incoming event: %s", json.dumps(event))

    headers = _lower_headers(event.get("headers") or {})

    channel_id = headers.get("x-goog-channel-id")
    channel_token = headers.get("x-goog-channel-token")
    resource_state = headers.get("x-goog-resource-state")
    resource_id = headers.get("x-goog-resource-id")
    resource_uri = headers.get("x-goog-resource-uri")
    message_number = headers.get("x-goog-message-number")
    channel_expiration = headers.get("x-goog-channel-expiration")

    if channel_token != EXPECTED_CHANNEL_TOKEN:
        logger.warning(
            "Rejected webhook: invalid token. got=%s expected=%s",
            channel_token,
            EXPECTED_CHANNEL_TOKEN
        )
        return {
            "statusCode": 403,
            "body": "forbidden"
        }

    if resource_state == "sync":
        logger.info(
            "Received sync notification. channel_id=%s resource_id=%s",
            channel_id,
            resource_id
        )
        return {
            "statusCode": 200,
            "body": "ok"
        }

    message = {
        "source": "google_drive_webhook",
        "channelId": channel_id,
        "channelToken": channel_token,
        "resourceState": resource_state,
        "resourceId": resource_id,
        "resourceUri": resource_uri,
        "messageNumber": message_number,
        "channelExpiration": channel_expiration,
    }

    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message)
    )

    logger.info(
        "Queued webhook event. resource_state=%s message_number=%s",
        resource_state,
        message_number
    )

    return {
        "statusCode": 200,
        "body": "ok"
    }