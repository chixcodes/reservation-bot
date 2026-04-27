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

    arabic_digits_map = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    date_str = (date_str or "").translate(arabic_digits_map).strip()
    time_str = (time_str or "").translate(arabic_digits_map).strip()

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

    dt_text = f"{normalized_date} {time_str}"
    print("GCAL normalized datetime:", dt_text)

    start = dtparse.parse(dt_text, dayfirst=True, fuzzy=True)

    if start.tzinfo is None:
        start = tz.localize(start)

    end = start + timedelta(minutes=duration_min)
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
    svc = _service()
    start_iso, end_iso = parse_when(date_str, time_str, duration_min)
    body = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_iso, "timeZone": TIMEZONE},
        "end":   {"dateTime": end_iso,   "timeZone": TIMEZONE},
    }

    if color_id:
        body["colorId"] = str(color_id)

    return svc.events().insert(calendarId=calendar_id, body=body).execute()

def delete_event(event_id, calendar_id="primary"):
    svc = _service()
    try:
        svc.events().delete(calendarId=calendar_id, eventId=event_id).execute()
        print(f"Google Calendar event deleted: {event_id}")
        return True
    except Exception as e:
        print("delete_event error:", e)
        return False
