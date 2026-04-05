import os
import re
import json
import logging
import boto3
import urllib.request
import urllib.parse
import urllib.error
from botocore.exceptions import ClientError

from google.oauth2 import service_account
from google.auth.transport.requests import Request

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ==============================================================================
# Environment
# ==============================================================================
SECRET_NAME = os.environ["SECRET_NAME"]
START_PAGE_TOKEN_PARAM = os.environ["START_PAGE_TOKEN_PARAM"]
DRIVE_ROOT_FOLDER_NAME = os.environ.get("DRIVE_ROOT_FOLDER_NAME", "2026")
AUTO_RENAME = os.environ.get("AUTO_RENAME", "true").strip().lower() == "true"
SHARED_DRIVE_ID = os.environ["SHARED_DRIVE_ID"]
SERVICE_ACCOUNT_SECRET_KEY = os.environ.get(
    "SERVICE_ACCOUNT_SECRET_KEY",
    "gdrive_service_account_json"
)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"

ARCHIVE_QUEUE_URL = os.environ["ARCHIVE_QUEUE_URL"]
ARCHIVE_EC2_INSTANCE_ID = os.environ["ARCHIVE_EC2_INSTANCE_ID"]
START_EC2_ON_JOB = os.environ.get("START_EC2_ON_JOB", "true").strip().lower() == "true"

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")

# ==============================================================================
# AWS clients
# ==============================================================================
sm = boto3.client("secretsmanager", region_name=AWS_REGION)
ssm = boto3.client("ssm", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)
ec2 = boto3.client("ec2", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

# ==============================================================================
# Extension groups
# ==============================================================================
VIDEO_EXTS = {
    "mp4", "mov", "mkv", "avi", "wmv", "flv", "webm", "m4v", "3gp", "mpeg", "mpg"
}
IMAGE_EXTS = {
    "png", "jpg", "jpeg", "webp", "bmp", "gif", "tif", "tiff", "heic", "heif"
}
TEXT_EXTS = {"txt"}
DIAGRAM_EXTS = {"drawio"}

# ==============================================================================
# Folder names exactly as they exist in Drive
# ==============================================================================
FOLDER_INTRO = "3. Introduction Video"
FOLDER_MOCK = "4. Mock Interview (First Call)"
FOLDER_PROJECT = "5. Project Scenarios"
FOLDER_NICHE = "6. 30 Questions Related to Their Niche"
FOLDER_RESUME = "7. 50 Questions Related to the Resume"
FOLDER_TOOLS = "8. Tools & Technology Videos"
FOLDER_SYSTEM_DESIGN = "9. System Design Video (with Draw.io)"
FOLDER_PERSONA = "10. Persona Video"
FOLDER_SMALL_TALK = "11. Small Talk"
FOLDER_JD = "12. JD Video"

# ==============================================================================
# Rename rules
# ==============================================================================
RENAME_RULES = {
    FOLDER_INTRO: {
        ("1", "video"): "Day3_Intro_<FullName>.<ext>",
    },
    FOLDER_MOCK: {
        ("1", "video"): "Day1_HR_<FullName>.<ext>",
    },
    FOLDER_PROJECT: {
        ("1", "video"): "Day2_Project_scenario_1_<FullName>.<ext>",
        ("2", "video"): "Day2_Project_scenario_2_<FullName>.<ext>",
        ("3", "video"): "Day2_Project_scenario_3_<FullName>.<ext>",
        ("4", "video"): "Day2_Project_scenario_4_<FullName>.<ext>",
        ("5", "video"): "Day2_Project_scenario_5_<FullName>.<ext>",
    },
    FOLDER_NICHE: {
        ("1", "video"): "Day1_Niche_<FullName>.<ext>",
    },
    FOLDER_RESUME: {
        ("1", "video"): "Day3_Resume_Mock_<FullName>.<ext>",
        ("2", "video"): "Day4_Resume_Mock_<FullName>.<ext>",
        ("3", "video"): "Day5_Resume_Mock_<FullName>.<ext>",
    },
    FOLDER_TOOLS: {
        ("1", "video"): "Day3_part1_Tools_System_<FullName>.<ext>",
        ("2", "video"): "Day3_part2_Tools_System_<FullName>.<ext>",
        ("3", "video"): "Day3_part3_Tools_System_<FullName>.<ext>",
        ("4", "video"): "Day3_Team_Structure_<FullName>.<ext>",
        ("4", "image"): "Day3_Team_Structure_<FullName>.<ext>",
        ("4", "drawio"): "Day3_Team_Structure_<FullName>.drawio",
    },
    FOLDER_SYSTEM_DESIGN: {
        ("1", "video"): "Day6_SystemDesign_Problem1_<FullName>.<ext>",
        ("1", "image"): "Day6_SystemDesign_Problem1_<FullName>.<ext>",
        ("1", "drawio"): "Day6_SystemDesign_Problem1_<FullName>.drawio",
    },
    FOLDER_PERSONA: {
        ("1", "video"): "Day4_Recruiter_<FullName>.<ext>",
        ("2", "video"): "Day4_Manager_<FullName>.<ext>",
        ("3", "video"): "Day4_Architect_<FullName>.<ext>",
    },
    FOLDER_SMALL_TALK: {
        ("1", "video"): "Day5_SmallTalk_<FullName>.<ext>",
    },
    FOLDER_JD: {
        ("1", "video"): "Day5_JD_Mapping_1_<FullName>.<ext>",
        ("1", "image"): "Day5_JD_Mapping_1_<FullName>.<ext>",
        ("1", "txt"): "Day5_JD_Mapping_1_<FullName>.txt",
        ("2", "video"): "Day5_JD_Mapping_2_<FullName>.<ext>",
        ("2", "image"): "Day5_JD_Mapping_2_<FullName>.<ext>",
        ("2", "txt"): "Day5_JD_Mapping_2_<FullName>.txt",
    },
}

# ==============================================================================
# Secret / auth helpers
# ==============================================================================
def get_secret_dict():
    resp = sm.get_secret_value(SecretId=SECRET_NAME)
    secret_str = resp.get("SecretString")
    if not secret_str:
        raise RuntimeError(f"SecretString is empty for secret: {SECRET_NAME}")
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
        raise RuntimeError("Failed to obtain Google access token from service account")

    return credentials.token


# ==============================================================================
# AWS helpers
# ==============================================================================
def get_ssm(name):
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]


def put_ssm(name, value):
    ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)


def ensure_archive_ec2_running():
    if not START_EC2_ON_JOB:
        return "disabled"

    if not ARCHIVE_EC2_INSTANCE_ID.strip():
        raise RuntimeError("ARCHIVE_EC2_INSTANCE_ID is empty")

    resp = ec2.describe_instances(InstanceIds=[ARCHIVE_EC2_INSTANCE_ID])
    reservations = resp.get("Reservations", [])
    if not reservations or not reservations[0].get("Instances"):
        raise RuntimeError(f"EC2 instance not found: {ARCHIVE_EC2_INSTANCE_ID}")

    state = reservations[0]["Instances"][0]["State"]["Name"]

    if state in {"running", "pending"}:
        return state

    if state in {"stopped", "stopping"}:
        ec2.start_instances(InstanceIds=[ARCHIVE_EC2_INSTANCE_ID])
        logger.info("Started archive EC2 instance: %s", ARCHIVE_EC2_INSTANCE_ID)
        return "starting"

    return state


# ==============================================================================
# HTTP helpers
# ==============================================================================
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


def http_patch_json(url, bearer_token, payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="PATCH")
    req.add_header("Authorization", f"Bearer {bearer_token}")
    req.add_header("Content-Type", "application/json")
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
        raise RuntimeError(f"HTTP {e.code} PATCH {url} :: {body}")


# ==============================================================================
# Name helpers
# ==============================================================================
def norm_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def underscored_name(s: str) -> str:
    s = norm_name(s)
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


def classify_ext(ext: str):
    ext = (ext or "").lower()
    if ext in VIDEO_EXTS:
        return "video"
    if ext in IMAGE_EXTS:
        return "image"
    if ext in TEXT_EXTS:
        return "txt"
    if ext in DIAGRAM_EXTS:
        return "drawio"
    return None


def split_ext(file_name: str):
    if not file_name or "." not in file_name:
        return file_name, None
    base, ext = file_name.rsplit(".", 1)
    return base, ext.lower()


def parse_simple_numbered_filename(file_name: str):
    if not file_name or "." not in file_name:
        return None, None

    m = re.fullmatch(r"\s*(\d+)\s*\.\s*([A-Za-z0-9]+)\s*", file_name)
    if not m:
        return None, None

    return m.group(1), m.group(2).lower()


def render_template(template: str, candidate_folder: str, ext: str):
    candidate_token = underscored_name(candidate_folder)
    return template.replace("<FullName>", candidate_token).replace("<ext>", ext)


def resolve_target_name(deliverable_folder: str, file_name: str, candidate_folder: str):
    rules = RENAME_RULES.get(deliverable_folder)
    if not rules:
        return None, "folder_not_managed"

    _, ext = split_ext(file_name)
    if not ext:
        return None, "missing_extension"

    ext_group = classify_ext(ext)
    if not ext_group:
        return None, "unsupported_extension"

    # Case 1: already final valid name
    for (_, rule_group), template in rules.items():
        if rule_group != ext_group:
            continue
        expected = render_template(template, candidate_folder, ext)
        if file_name == expected:
            return expected, "already_valid"

    # Case 2: numbered upload like 1.mp4 / 2.txt
    number, parsed_ext = parse_simple_numbered_filename(file_name)
    if not number or not parsed_ext:
        return None, "invalid_filename_for_folder"

    parsed_group = classify_ext(parsed_ext)
    if not parsed_group:
        return None, "unsupported_extension"

    template = rules.get((number, parsed_group))
    if not template:
        return None, "no_matching_numbered_rule"

    return render_template(template, candidate_folder, parsed_ext), "numbered_rule"


# ==============================================================================
# Drive metadata helpers
# ==============================================================================
def get_file_meta(file_id, access_token):
    url = (
        f"https://www.googleapis.com/drive/v3/files/{file_id}"
        "?supportsAllDrives=true"
        "&fields=id,name,mimeType,parents,driveId,trashed"
    )
    try:
        return http_get_json(url, access_token)
    except RuntimeError as e:
        if "HTTP 404" in str(e):
            return None
        raise


def get_parent(file_id, access_token):
    url = (
        f"https://www.googleapis.com/drive/v3/files/{file_id}"
        "?supportsAllDrives=true"
        "&fields=id,name,parents,driveId"
    )
    try:
        return http_get_json(url, access_token)
    except RuntimeError as e:
        if "HTTP 404" in str(e):
            return None
        raise


def get_ancestry(file_id, access_token):
    chain = []
    current_id = file_id
    seen = set()

    while current_id and current_id not in seen:
        seen.add(current_id)
        f = get_parent(current_id, access_token)
        if not f:
            return None
        chain.append({"id": f["id"], "name": f["name"]})
        parents = f.get("parents", [])
        current_id = parents[0] if parents else None

    return list(reversed(chain))


def resolve_context_from_ancestry(ancestry):
    names = [x["name"] for x in ancestry]

    try:
        root_idx = names.index(DRIVE_ROOT_FOLDER_NAME)
    except ValueError:
        return None

    relative = names[root_idx:]
    if len(relative) < 5:
        return None

    return {
        "root_folder": relative[0],
        "slot_folder": relative[1],
        "candidate_folder": relative[2],
        "deliverable_folder": relative[3],
        "file_name": relative[4],
    }


def rename_file(file_id, new_name, access_token):
    url = (
        f"https://www.googleapis.com/drive/v3/files/{file_id}"
        "?supportsAllDrives=true"
        "&fields=id,name"
    )
    return http_patch_json(url, access_token, {"name": new_name})


# ==============================================================================
# Queue / S3 helpers
# ==============================================================================
def join_s3_key(*parts):
    cleaned = []
    for part in parts:
        if part is None:
            continue
        part = str(part).strip("/")
        if part:
            cleaned.append(part)
    return "/".join(cleaned)


def build_s3_key(ctx, final_name):
    return join_s3_key(
        S3_PREFIX,
        ctx["root_folder"],
        ctx["slot_folder"],
        ctx["candidate_folder"],
        ctx["deliverable_folder"],
        final_name,
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


def enqueue_archive_job(file_id, final_name, ctx):
    message = {
        "fileId": file_id,
        "rootFolder": ctx["root_folder"],
        "slotFolder": ctx["slot_folder"],
        "candidateFolder": ctx["candidate_folder"],
        "deliverableFolder": ctx["deliverable_folder"],
        "finalFileName": final_name,
        "sharedDriveId": SHARED_DRIVE_ID,
    }

    sqs.send_message(
        QueueUrl=ARCHIVE_QUEUE_URL,
        MessageBody=json.dumps(message),
    )
    return message


# ==============================================================================
# Changes API
# ==============================================================================
def list_changes(page_token, access_token):
    url = (
        "https://www.googleapis.com/drive/v3/changes"
        f"?pageToken={urllib.parse.quote(page_token)}"
        f"&driveId={urllib.parse.quote(SHARED_DRIVE_ID)}"
        "&supportsAllDrives=true"
        "&includeItemsFromAllDrives=true"
        "&restrictToMyDrive=false"
        "&includeRemoved=true"
        "&pageSize=100"
        "&fields=nextPageToken,newStartPageToken,"
        "changes(fileId,removed,file(id,name,mimeType,parents,driveId,trashed))"
    )
    return http_get_json(url, access_token)


def process_changes(access_token):
    page_token = get_ssm(START_PAGE_TOKEN_PARAM)

    renamed = []
    queued = []
    skipped = []

    while True:
        data = list_changes(page_token, access_token)
        next_page_token = data.get("nextPageToken")
        new_start_page_token = data.get("newStartPageToken")

        for ch in data.get("changes", []):
            if ch.get("removed"):
                skipped.append({
                    "fileId": ch.get("fileId"),
                    "reason": "removed_change"
                })
                continue

            file_obj = ch.get("file") or {}
            file_id = file_obj.get("id") or ch.get("fileId")
            if not file_id:
                skipped.append({"reason": "missing_file_id"})
                continue

            file_meta = get_file_meta(file_id, access_token)
            if not file_meta:
                skipped.append({
                    "fileId": file_id,
                    "reason": "file_not_found_or_no_access"
                })
                continue

            if file_meta.get("driveId") and file_meta.get("driveId") != SHARED_DRIVE_ID:
                skipped.append({
                    "fileId": file_id,
                    "reason": "outside_target_shared_drive",
                    "name": file_meta.get("name")
                })
                continue

            if file_meta.get("trashed"):
                skipped.append({
                    "fileId": file_id,
                    "reason": "trashed",
                    "name": file_meta.get("name")
                })
                continue

            if file_meta.get("mimeType") == "application/vnd.google-apps.folder":
                skipped.append({
                    "fileId": file_id,
                    "reason": "folder_change",
                    "name": file_meta.get("name")
                })
                continue

            ancestry = get_ancestry(file_id, access_token)
            if not ancestry:
                skipped.append({
                    "fileId": file_id,
                    "reason": "ancestry_not_found",
                    "name": file_meta.get("name")
                })
                continue

            ctx = resolve_context_from_ancestry(ancestry)
            if not ctx:
                skipped.append({
                    "fileId": file_id,
                    "reason": "outside_root_path",
                    "name": file_meta.get("name")
                })
                continue

            deliverable_folder = ctx["deliverable_folder"]
            candidate_folder = ctx["candidate_folder"]
            actual_name = ctx["file_name"]

            target_name, reason = resolve_target_name(
                deliverable_folder=deliverable_folder,
                file_name=actual_name,
                candidate_folder=candidate_folder
            )

            if not target_name:
                skipped.append({
                    "fileId": file_id,
                    "reason": reason,
                    "name": actual_name,
                    "folder": deliverable_folder
                })
                continue

            final_name = actual_name

            if actual_name != target_name:
                if AUTO_RENAME:
                    updated = rename_file(file_id, target_name, access_token)
                    final_name = updated.get("name") or target_name
                    renamed.append({
                        "fileId": file_id,
                        "oldName": actual_name,
                        "newName": final_name,
                        "slotFolder": ctx["slot_folder"],
                        "candidateFolder": candidate_folder,
                        "deliverableFolder": deliverable_folder
                    })
                else:
                    skipped.append({
                        "fileId": file_id,
                        "reason": "auto_rename_disabled",
                        "name": actual_name,
                        "targetName": target_name,
                        "folder": deliverable_folder
                    })
                    continue
            else:
                final_name = target_name

            s3_key = build_s3_key(ctx, final_name)
            if s3_object_exists(S3_BUCKET, s3_key):
                skipped.append({
                    "fileId": file_id,
                    "reason": "already_archived_in_s3",
                    "name": final_name,
                    "folder": deliverable_folder,
                    "s3Key": s3_key
                })
                continue

            enqueue_archive_job(
                file_id=file_id,
                final_name=final_name,
                ctx=ctx,
            )

            ec2_state = ensure_archive_ec2_running()

            queued.append({
                "fileId": file_id,
                "name": final_name,
                "slotFolder": ctx["slot_folder"],
                "candidateFolder": candidate_folder,
                "deliverableFolder": deliverable_folder,
                "queueUrl": ARCHIVE_QUEUE_URL,
                "ec2State": ec2_state,
                "s3Key": s3_key
            })

        if next_page_token:
            page_token = next_page_token
            continue

        if new_start_page_token:
            put_ssm(START_PAGE_TOKEN_PARAM, new_start_page_token)
            logger.info("Updated SSM start page token")

        break

    return renamed, queued, skipped


# ==============================================================================
# Lambda handler
# ==============================================================================
def lambda_handler(event, context):
    logger.info(
        "Starting drive_change_processor | root=%s | sharedDriveId=%s | autoRename=%s",
        DRIVE_ROOT_FOLDER_NAME,
        SHARED_DRIVE_ID,
        AUTO_RENAME,
    )

    try:
        access_token = get_google_access_token()
        renamed, queued, skipped = process_changes(access_token)

        logger.info(
            "Completed drive_change_processor | renamed=%s | queued=%s | skipped=%s",
            len(renamed),
            len(queued),
            len(skipped),
        )

        return {
            "ok": True,
            "mode": "shared_drive_service_account",
            "sharedDriveId": SHARED_DRIVE_ID,
            "renamedCount": len(renamed),
            "renamed": renamed[:50],
            "queuedCount": len(queued),
            "queued": queued[:50],
            "skippedCount": len(skipped),
            "skipped": skipped[:100],
        }

    except Exception as exc:
        logger.exception("Unhandled exception in drive_change_processor")
        return {
            "ok": False,
            "errorType": type(exc).__name__,
            "error": str(exc),
        }