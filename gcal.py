# gcal.py
import os
from datetime import datetime, timedelta
import pytz
from dateutil import parser as dtparse
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/calendar"]
TIMEZONE = "Asia/Beirut"

def _service():
    creds = None

    # Load existing token if it exists
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # If no creds or invalid, refresh or do full OAuth
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # FIX: pass a Request() object here
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        # Save token
        with open("token.json", "w") as f:
            f.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


def parse_when(date_str, time_str, duration_min=45):
    tz = pytz.timezone(TIMEZONE)
    start = dtparse.parse(f"{date_str} {time_str}", dayfirst=False)  # accept "2025-11-10 6 PM" etc.
    if start.tzinfo is None:
        start = tz.localize(start)
    end = start + timedelta(minutes=duration_min)
    return start.isoformat(), end.isoformat()

def create_event(summary, date_str, time_str, description="", calendar_id="primary", duration_min=45):
    svc = _service()
    start_iso, end_iso = parse_when(date_str, time_str, duration_min)
    body = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_iso, "timeZone": TIMEZONE},
        "end":   {"dateTime": end_iso,   "timeZone": TIMEZONE},
    }
    return svc.events().insert(calendarId=calendar_id, body=body).execute()
