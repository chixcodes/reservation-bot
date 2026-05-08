# gcal.py
import os
import json
import tempfile
import atexit
from datetime import datetime, timedelta
import pytz
from dateutil import parser as dtparse
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/calendar"]
TIMEZONE = "Asia/Beirut"
_TEMP_FILES = []


def _cleanup_temp_files():
    for path in _TEMP_FILES:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


atexit.register(_cleanup_temp_files)


def _is_render_environment():
    return bool(
        os.getenv("RENDER")
        or os.getenv("RENDER_SERVICE_ID")
        or os.getenv("RENDER_EXTERNAL_URL")
    )


def _get_credentials_file_path():
    env_json = (os.getenv("GOOGLE_CREDENTIALS_JSON") or "").strip()
    if env_json:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix="_google_credentials.json")
        tmp.write(env_json.encode("utf-8"))
        tmp.flush()
        tmp.close()
        _TEMP_FILES.append(tmp.name)
        return tmp.name, "env_json"

    env_path = (os.getenv("GOOGLE_CREDENTIALS_PATH") or "").strip()
    if env_path and os.path.exists(env_path):
        return env_path, "env_path"

    if os.path.exists("credentials.json"):
        return "credentials.json", "local_file"

    return None, None


def _load_token_credentials():
    env_token_json = (os.getenv("GOOGLE_TOKEN_JSON") or "").strip()
    if env_token_json:
        try:
            info = json.loads(env_token_json)
            return Credentials.from_authorized_user_info(info, SCOPES), "env_json", None
        except Exception as e:
            print("GCAL token env parse error:", e)

    env_token_path = (os.getenv("GOOGLE_TOKEN_PATH") or "").strip()
    if env_token_path and os.path.exists(env_token_path):
        return Credentials.from_authorized_user_file(env_token_path, SCOPES), "env_path", env_token_path

    if os.path.exists("token.json"):
        return Credentials.from_authorized_user_file("token.json", SCOPES), "local_file", "token.json"

    return None, None, None


def get_calendar_connection_status():
    creds, token_source, _ = _load_token_credentials()
    credentials_path, credentials_source = _get_credentials_file_path()

    if not creds:
        if credentials_path:
            return False, "No Google token found on the server. Add token.json or GOOGLE_TOKEN_JSON."
        return False, "Missing Google credentials.json and token.json on the server."

    try:
        if creds.valid:
            return True, f"Connected via {token_source or 'token'}"
        if creds.expired and creds.refresh_token:
            return True, f"Refreshable token via {token_source or 'token'}"
        return False, "Google token exists but is not valid and cannot be refreshed."
    except Exception as e:
        return False, f"Google Calendar status check failed: {e}"


def is_google_calendar_connected():
    connected, _ = get_calendar_connection_status()
    return connected


def _service(allow_interactive=False):
    creds, token_source, token_path = _load_token_credentials()
    credentials_path, credentials_source = _get_credentials_file_path()

    print(
        "GCAL credential sources:",
        {
            "token_source": token_source,
            "credentials_source": credentials_source,
            "render": _is_render_environment(),
        },
    )

    if creds and not creds.valid:
        if creds.expired and creds.refresh_token:
            print("GCAL refreshing expired token")
            creds.refresh(Request())
            if token_path:
                try:
                    with open(token_path, "w") as f:
                        f.write(creds.to_json())
                except Exception as e:
                    print("GCAL token save warning:", e)
        else:
            creds = None

    if not creds:
        if not allow_interactive or _is_render_environment():
            connected, reason = get_calendar_connection_status()
            raise RuntimeError(reason)

        if not credentials_path:
            raise RuntimeError("Missing credentials.json for Google Calendar OAuth.")

        flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
        creds = flow.run_local_server(port=0)

        try:
            with open("token.json", "w") as f:
                f.write(creds.to_json())
        except Exception as e:
            print("GCAL token save warning:", e)

    return build("calendar", "v3", credentials=creds)


def parse_when(date_str, time_str, duration_min=45):
    tz = pytz.timezone(TIMEZONE)

    arabic_digits_map = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    date_str = (date_str or "").translate(arabic_digits_map).strip()
    time_str = (time_str or "").translate(arabic_digits_map).strip().upper().replace(".", "")

    month_map = {
        "كانون الثاني": "January",
        "يناير": "January",
        "شباط": "February",
        "فبراير": "February",
        "آذار": "March",
        "اذار": "March",
        "مارس": "March",
        "نيسان": "April",
        "ابريل": "April",
        "أبريل": "April",
        "أيار": "May",
        "مايو": "May",
        "حزيران": "June",
        "يونيو": "June",
        "تموز": "July",
        "يوليو": "July",
        "آب": "August",
        "اغسطس": "August",
        "أغسطس": "August",
        "أيلول": "September",
        "سبتمبر": "September",
        "تشرين الأول": "October",
        "اكتوبر": "October",
        "أكتوبر": "October",
        "تشرين الثاني": "November",
        "نوفمبر": "November",
        "كانون الأول": "December",
        "ديسمبر": "December",
    }

    normalized_date = date_str
    for ar, en in month_map.items():
        normalized_date = normalized_date.replace(ar, en)

    if time_str.endswith("AM") or time_str.endswith("PM"):
        if len(time_str) > 2 and time_str[-3] != " ":
            time_str = time_str[:-2] + " " + time_str[-2:]

    parsed_time = None
    for fmt in ("%H:%M", "%H", "%I %p", "%I:%M %p"):
        try:
            parsed_time = datetime.strptime(time_str, fmt).time()
            break
        except Exception:
            continue

    if parsed_time is None:
        raise ValueError(f"Could not parse time: {time_str}")

    parsed_date = dtparse.parse(normalized_date, dayfirst=True, fuzzy=True).date()
    start = tz.localize(datetime.combine(parsed_date, parsed_time))
    end = start + timedelta(minutes=duration_min)

    print("GCAL start_iso:", start.isoformat())
    print("GCAL end_iso:", end.isoformat())

    return start.isoformat(), end.isoformat()


def create_event(

    summary,
    date_str,
    time_str,
    description="",
    calendar_id="primary",
    duration_min=45,
    color_id=None,
):
    svc = _service(allow_interactive=False)
    start_iso, end_iso = parse_when(date_str, time_str, duration_min)
    body = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_iso, "timeZone": TIMEZONE},
        "end": {"dateTime": end_iso, "timeZone": TIMEZONE},
    }

    if color_id:
        body["colorId"] = str(color_id)

    print("GCAL event body:", body)
    print("GCAL create_event calendar_id:", calendar_id, flush=True)
    created = svc.events().insert(calendarId=calendar_id, body=body).execute()
    print("GCAL created event id:", created.get("id"), flush=True)
    print("GCAL created event htmlLink:", created.get("htmlLink"), flush=True)
    return created
    return svc.events().insert(calendarId=calendar_id, body=body).execute()


def delete_event(event_id, calendar_id="primary"):
    svc = _service(allow_interactive=False)
    try:
        svc.events().delete(calendarId=calendar_id, eventId=event_id).execute()
        print(f"Google Calendar event deleted: {event_id}")
        return True
    except Exception as e:
        print("delete_event error:", e)
        return False
