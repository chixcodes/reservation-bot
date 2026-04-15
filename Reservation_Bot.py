# Reservation_Bot.py — CLEANED VERSION

import os
import psycopg2
from db_utils import get_db_connection, init_db
import requests
import json
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from gcal import create_event, delete_event
from dotenv import load_dotenv
from flask import (
    Flask,
    request,
    render_template,
    render_template_string,
    session,
    redirect,
    url_for,
    jsonify,
)
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from dateutil import parser as dtparse
import pytz
import sys

# ------------------ DB CONFIG ------------------
DB_FILENAME = "reservation_v2.db"###
# ------------------ BUSINESS HELPERS ------------------


def get_business_by_phone_number_id(phone_number_id: str):
    conn = get_db_connection()
    c = conn.cursor()

    # Try to match by phone_number_id
    c.execute(
        "SELECT * FROM businesses WHERE phone_number_id=%s LIMIT 1",
        (phone_number_id,),
    )
    row = c.fetchone()

    # If nothing found, fallback to first business (for dev)
    if not row:
        c.execute("SELECT * FROM businesses LIMIT 1")
        row = c.fetchone()

    conn.close()
    if row:
        return dict(row)
    return None


def get_business_by_id(business_id: int):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM businesses WHERE id=%s", (business_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return dict(row)
    return None


# ------------------ FLASK APP ------------------

app = Flask(__name__)
app.secret_key = "CHANGE_THIS_SECRET_KEY"  # change in production

# ------------------ CONFIG (.env) ------------------

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "khoury123")


# ------------------ CONVERSATION STATE ------------------

user_state = {}  # key: (business_id, phone) -> dict


SERVICE_KEYWORDS = {
    # English
    "haircut": "Haircut",
    "cut": "Haircut",
    "coupe": "Haircut",
    "beard": "Beard Trim",
    "barbe": "Beard Trim",
    "shave": "Beard Trim",
    "color": "Hair Coloring",
    "colour": "Hair Coloring",
    "dye": "Hair Coloring",
    # Arabic – Haircut
    "قص شعر": "Haircut",
    "قصة شعر": "Haircut",
    "حلاقة شعر": "Haircut",
    "حلاقة": "Haircut",
    # Arabic – Beard
    "ذقن": "Beard Trim",
    "تهذيب ذقن": "Beard Trim",
    "حلاقة دقن": "Beard Trim",
    # Arabic – Coloring
    "صبغ": "Hair Coloring",
    "صبغة": "Hair Coloring",
    "صبغ شعر": "Hair Coloring",
}


# ------------------ LOW-LEVEL HELPERS ------------------


def send_message(to: str, text: str, business: dict):

    if not business.get("phone_number_id") or not business.get("access_token"):
        print("send_message: missing phone_number_id or access_token for business")
        return

    url = f"https://graph.facebook.com/v21.0/{business['phone_number_id']}/messages"
    headers = {
        "Authorization": f"Bearer {business['access_token']}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text},
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        print("send_message: meta", r.status_code, r.text)
    except Exception as e:
        print("send_message error (meta):", e)
    print("send_message status:", r.status_code, flush=True)
    print("send_message body:", r.text, flush=True)

def ai_pick_service(business: dict, user_text: str):
    """
    Use OpenRouter to map free-text to a service name from this business's services.
    Returns service name or None.
    """
    if not OPENROUTER_API_KEY:
        print("ai_pick_service: no OPENROUTER_API_KEY set")
        return None

    services = get_service_names_for_business(business["id"])
    if not services:
        print("ai_pick_service: no services configured for business", business["id"])
        return None

    services_str = ", ".join(services)

    system_msg = (
        "You help map customer booking messages to a single service name.\n"
        "The customer may write in Arabic, English, or French, with slang.\n"
        "You are given a list of valid services for this business.\n"
        "Always answer with pure JSON: {\"service\": \"Name\"}.\n"
        f"Valid services: {services_str}"
    )
    user_msg = f"Customer message: {user_text}"

    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENROUTER_MODEL,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
            },
            timeout=12,
        )
        print("ai_pick_service status:", resp.status_code)
        if not resp.ok:
            print("ai_pick_service body:", resp.text)
            return None

        data = resp.json()
        if "choices" not in data or not data["choices"]:
            return None

        content = data["choices"][0]["message"]["content"]
        obj = json.loads(content)
        service = obj.get("service")
        if isinstance(service, str) and service.strip():
            return service.strip()

    except Exception as e:
        print("ai_pick_service error:", e)

    return None


def get_service_info(business_id, service_name):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT price, duration_min
        FROM services
        WHERE business_id = %s AND lower(name) = lower(%s)
        """,
        (business_id, service_name),
    )
    row = c.fetchone()
    conn.close()

    if row:
        return {
            "price": float(row["price"] or 0),
            "duration": int(row["duration_min"] or 45),
        }

    return {"price": 0.0, "duration": 45}


def get_service_names_for_business(business_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT name FROM services WHERE business_id = %s", (business_id,))
    rows = c.fetchall()
    conn.close()
    return [r["name"] for r in rows]


def normalize_time_str(tstr: str):
    tstr = tstr.strip().upper().replace(".", "")

    candidate = None
    for part in tstr.split():
        if any(ch.isdigit() for ch in part):
            candidate = part
    if candidate is None:
        return None
    tstr = candidate

    if tstr.endswith("AM") or tstr.endswith("PM"):
        if len(tstr) > 2 and tstr[-3] != " ":
            tstr = tstr[:-2] + " " + tstr[-2:]

    for fmt in ["%H:%M", "%I %p", "%I:%M %p", "%H"]:
        try:
            t = datetime.strptime(tstr, fmt).time()
            return f"{t.hour:02d}:{t.minute:02d}"
        except Exception:
            continue

    return None


def suggest_slots(
    date_str,
    requested_time_str,
    open_start="09:00",
    open_end="18:00",
    step_min=30,
    max_suggestions=3,
):
    norm_req = normalize_time_str(requested_time_str)
    if norm_req is None:
        norm_req = open_start

    req_time = datetime.strptime(norm_req, "%H:%M").time()
    start = datetime.strptime(open_start, "%H:%M").time()
    end = datetime.strptime(open_end, "%H:%M").time()

    base = datetime.combine(datetime.today(), start)
    end_dt = datetime.combine(datetime.today(), end)

    slots = []
    while base <= end_dt:
        slots.append(base.time())
        base += timedelta(minutes=step_min)

    free = []
    for t in slots:
        hhmm = f"{t.hour:02d}:{t.minute:02d}"
        if not is_slot_taken(date_str, hhmm):
            free.append(t)

    def minutes(t):
        return t.hour * 60 + t.minute

    target = minutes(req_time)
    free.sort(key=lambda t: abs(minutes(t) - target))

    return [f"{t.hour:02d}:{t.minute:02d}" for t in free[:max_suggestions]]


def save_reservation(business_id, phone, name, service, date, time_):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO reservations (business_id, customer_name, customer_phone, service, date, time, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'CONFIRMED')
            RETURNING id
            """,
            (business_id, name, phone, service, date, time_),
        )
        row = c.fetchone()
        new_id = row["id"] if isinstance(row, dict) else row[0]
        conn.commit()
        print(f"SAVED (CONFIRMED) -> id={new_id}, {name}, {service} on {date} at {time_}")
        return new_id
    except Exception as e:
        conn.rollback()
        print("save_reservation error:", str(e))
        raise
    finally:
        conn.close()

def is_slot_taken(business_id, date, time_):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT 1
        FROM reservations
        WHERE business_id = %s
          AND date = %s
          AND time = %s
          AND status = 'CONFIRMED'
        LIMIT 1
        """,
        (business_id, date, time_),
    )
    row = c.fetchone()
    conn.close()
    return row is not None


def send_reservation_confirmation(phone, name, service, date, time, business, calendar_added=False):
    service_info = get_service_info(business["id"], service)
    total_price = service_info["price"]

    calendar_line = "\n🗓 Also added to our Google Calendar." if calendar_added else ""

    message = (
        f"✅ Your reservation is confirmed!\n"
        f"Name: {name}\n"
        f"Service: {service}\n"
        f"Date: {date}\n"
        f"Time: {time}\n"
        f"Total Price: ${total_price:.2f}"
        f"{calendar_line}\n\n"
        f"Thank you for booking with us 🤍"
    )
    send_message(phone, message, business)


def send_reservation_cancellation(phone, name, service, date, time, business):
    message = (
        f"❌ Your reservation has been canceled.\n"
        f"Name: {name}\n"
        f"Service: {service}\n"
        f"Date: {date}\n"
        f"Time: {time}\n\n"
        f"If this is a mistake, please contact us to reschedule."
    )
    send_message(phone, message, business)

def add_reservation_to_google_calendar(name, service, date, time_):
    try:
        summary = f"{service} - {name}"
        description = (
            f"Customer: {name}\n"
            f"Service: {service}\n"
            f"Date: {date}\n"
            f"Time: {time_}"
        )

        event = create_event(
            summary=summary,
            date_str=date,
            time_str=time_,
            description=description,
            calendar_id="primary",
            duration_min=45,
        )

        print("Google Calendar event created:", event.get("id"))
        return event

    except Exception as e:
        print("add_reservation_to_google_calendar error:", str(e))
        return None

def save_google_event_id(reservation_id, google_event_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        UPDATE reservations
        SET google_event_id = %s
        WHERE id = %s
        """,
        (google_event_id, reservation_id),
    )
    conn.commit()
    conn.close()


def get_confirmed_reservations_for_phone(business_id, phone):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, google_event_id, customer_name, service, date, time
        FROM reservations
        WHERE business_id = %s
          AND customer_phone = %s
          AND status = 'CONFIRMED'
        ORDER BY id DESC
        """,
        (business_id, phone),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def mark_reservations_cancelled_by_phone(business_id, phone):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        UPDATE reservations
        SET status = 'CANCELLED'
        WHERE business_id = %s
          AND customer_phone = %s
          AND status = 'CONFIRMED'
        """,
        (business_id, phone),
    )
    conn.commit()
    affected = c.rowcount
    conn.close()
    return affected

# ------------------ CONVERSATION LOGIC ------------------
import re

def detect_lang(text):
    t = (text or "").strip()

    # Arabic letters
    if re.search(r'[\u0600-\u06FF]', t):
        return "ar"

    lt = t.lower()
    french_markers = ["bonjour", "salut", "réserver", "reservation", "annuler", "merci"]
    if any(w in lt for w in french_markers):
        return "fr"

    return "en"


def is_booking_intent(text):
    t = (text or "").strip().lower()
    booking_keywords = [
        "book", "booking", "reserve", "reservation", "appointment",
        "bonjour je veux reserver", "réserver", "reserver", "rdv",
        "احجز", "أحجز", "حجز", "موعد", "بدي احجز", "اريد احجز",
        "bede ehjoz", "bade ehjoz", "ehjoz", "ehجز", "7جز", "7joz"
    ]
    return any(k in t for k in booking_keywords)


def is_cancel_intent(text):
    t = (text or "").strip().lower()
    cancel_keywords = [
        "cancel", "cancellation", "annuler", "annule", "supprimer reservation",
        "الغاء", "إلغاء", "الغي", "بدي الغي",
        "bede elghe", "bade elghe", "elghe", "elghi"
    ]
    return any(k in t for k in cancel_keywords)


def tr(lang, key, **kwargs):
    messages = {
        "greeting": {
            "en": "Hi! Welcome 👋\n\nHow can I help you today?\n• Type *book* to make a reservation\n• Type *cancel* to cancel your reservation",
            "ar": "أهلاً 👋\n\nكيف فيني ساعدك اليوم؟\n• اكتب *احجز* لتعمل حجز\n• اكتب *الغاء* لتلغي الحجز",
            "fr": "Bonjour 👋\n\nComment puis-je vous aider aujourd’hui ?\n• Tapez *book* pour réserver\n• Tapez *cancel* pour annuler votre réservation",
        },
        "ask_name": {
            "en": "Sure — what is your full name?",
            "ar": "أكيد 🤍 شو الاسم الكامل للحجز؟",
            "fr": "Bien sûr — quel est votre nom complet ?",
        },
        "ask_service": {
            "en": "Thanks, {name}. Which service would you like? (e.g., haircut, consultation)",
            "ar": "شكراً {name}. أي خدمة بدك؟ (مثلاً: قص شعر، استشارة)",
            "fr": "Merci, {name}. Quel service souhaitez-vous ? (ex. coupe, consultation)",
        },
        "ask_date": {
            "en": "Great — {service}. What date would you like? (e.g., 2026-04-20)",
            "ar": "ممتاز — {service}. أي تاريخ بدك؟ (مثلاً: 2026-04-20)",
            "fr": "Parfait — {service}. Quelle date souhaitez-vous ? (ex. 2026-04-20)",
        },
        "ask_time": {
            "en": "Perfect — and what time? (e.g., 16:00 or 4 PM)",
            "ar": "ممتاز — وأي ساعة؟ (مثلاً: 16:00 أو 4 PM)",
            "fr": "Parfait — à quelle heure ? (ex. 16:00 ou 4 PM)",
        },
        "invalid_time": {
            "en": "Please send a valid time, like 16:00 or 4 PM.",
            "ar": "من فضلك ابعت وقت صحيح، مثل 16:00 أو 4 PM.",
            "fr": "Veuillez envoyer une heure valide, comme 16:00 ou 4 PM.",
        },
        "slot_taken": {
            "en": "Sorry, {date} at {time} is already booked. Please choose another time 🤍",
            "ar": "عذراً، الموعد {date} الساعة {time} محجوز. اختار وقت تاني 🤍",
            "fr": "Désolé, le créneau du {date} à {time} est déjà réservé. Choisissez une autre heure 🤍",
        },
        "no_active_cancel": {
            "en": "You have no active reservations to cancel.",
            "ar": "ما عندك أي حجوزات مفعّلة لتلغيها.",
            "fr": "Vous n’avez aucune réservation active à annuler.",
        },
        "cancel_done": {
            "en": "✅ Cancelled {count} reservation(s).\n🗓 Removed {events} event(s) from Google Calendar.",
            "ar": "✅ تم إلغاء {count} حجز/حجوزات.\n🗓 وتم حذف {events} موعد/مواعيد من Google Calendar.",
            "fr": "✅ {count} réservation(s) annulée(s).\n🗓 {events} événement(s) supprimé(s) de Google Calendar.",
        },
        "save_error": {
            "en": "Something went wrong while saving your reservation. Please try again.",
            "ar": "صار خطأ أثناء حفظ الحجز. جرب مرة ثانية.",
            "fr": "Une erreur s’est produite أثناء حفظ la réservation. Veuillez réessayer.",
        },
        "invalid_date": {
            "en": "Please send a valid date, like 2026-04-20 or 20 April.",
            "ar": "من فضلك ابعت تاريخ صحيح، مثل 2026-04-20 أو 20 نيسان.",
            "fr": "Veuillez envoyer une date valide, comme 2026-04-20 ou 20 avril.",
        },
        "closed_day": {
            "en": "Sorry, we’re closed on that day. Please choose another date.",
            "ar": "عذراً، نحن مغلقون في هذا اليوم. اختار تاريخ تاني.",
            "fr": "Désolé, nous sommes fermés ce jour-là. Choisissez une autre date.",
        },
        "outside_hours": {
            "en": "That time is outside business hours. Please choose another time.",
            "ar": "هذا الوقت خارج ساعات العمل. اختار وقت تاني.",
            "fr": "Cette heure est en dehors des horaires d’ouverture. Choisissez une autre heure.",
        },
    }

    lang_messages = messages.get(key, {})
    template = lang_messages.get(lang) or lang_messages.get("en") or key
    return template.format(**kwargs)
from datetime import datetime
from dateutil import parser as dtparse
import pytz

WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def normalize_booking_date(date_str):
    tz = pytz.timezone("Asia/Beirut")

    arabic_digits_map = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    date_str = (date_str or "").translate(arabic_digits_map).strip()

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

    normalized = date_str
    for ar, en in month_map.items():
        normalized = normalized.replace(ar, en)

    dt = dtparse.parse(normalized, dayfirst=True, fuzzy=True)

    if dt.tzinfo is None:
        dt = tz.localize(dt)

    return dt.date().isoformat()


def ensure_default_hours(business_id):
    conn = get_db_connection()
    c = conn.cursor()

    defaults = [
        (0, False, "09:00", "18:00"),
        (1, False, "09:00", "18:00"),
        (2, False, "09:00", "18:00"),
        (3, False, "09:00", "18:00"),
        (4, False, "09:00", "18:00"),
        (5, False, "09:00", "18:00"),
        (6, True, None, None),
    ]

    for weekday, is_closed, open_time, close_time in defaults:
        c.execute(
            """
            INSERT INTO business_hours (business_id, weekday, is_closed, open_time, close_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (business_id, weekday) DO NOTHING
            """,
            (business_id, weekday, is_closed, open_time, close_time),
        )

    conn.commit()
    conn.close()


def get_day_rules(business_id, date_iso):
    target_date = datetime.strptime(date_iso, "%Y-%m-%d").date()
    weekday = target_date.weekday()

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT id
        FROM blocked_dates
        WHERE business_id = %s AND blocked_date = %s
        """,
        (business_id, date_iso),
    )
    blocked = c.fetchone()
    if blocked:
        conn.close()
        return {"blocked": True, "closed": True, "reason": "blocked_date"}

    c.execute(
        """
        SELECT weekday, is_closed,
               TO_CHAR(open_time, 'HH24:MI') AS open_time,
               TO_CHAR(close_time, 'HH24:MI') AS close_time
        FROM business_hours
        WHERE business_id = %s AND weekday = %s
        """,
        (business_id, weekday),
    )
    row = c.fetchone()
    conn.close()

    if not row:
        return {"blocked": False, "closed": False, "open_time": "09:00", "close_time": "18:00"}

    if row["is_closed"]:
        return {"blocked": False, "closed": True, "reason": "weekly_closed"}

    return {
        "blocked": False,
        "closed": False,
        "open_time": row["open_time"],
        "close_time": row["close_time"],
    }


def is_time_within_business_hours(time_str, open_time, close_time):
    chosen = datetime.strptime(time_str, "%H:%M").time()
    start = datetime.strptime(open_time, "%H:%M").time()
    end = datetime.strptime(close_time, "%H:%M").time()
    return start <= chosen <= end


def process_incoming_message(business, phone, text):
    global user_state

    t = (text or "").strip()
    lt = t.lower()

    key = (business["id"], phone)
    state = user_state.get(key)

    lang = state.get("lang") if state and state.get("lang") else detect_lang(t)

    # GREETING
    if lt in [
        "hi", "hello", "hey", "hii", "heyy", "hola",
        "bonjour", "salut",
        "مرحبا", "اهلا", "أهلا", "سلام",
        "hi kifak", "hi kifik", "kifak", "kifik"
    ]:
        send_message(phone, tr(lang, "greeting"), business)
        return "ok", 200

    # START BOOKING
    if is_booking_intent(lt):
        user_state[key] = {"step": "awaiting_name", "lang": lang}
        send_message(phone, tr(lang, "ask_name"), business)
        return "ok", 200

    # CANCEL BOOKING
    if is_cancel_intent(lt):
        reservations = get_confirmed_reservations_for_phone(business["id"], phone)

        if not reservations:
            send_message(phone, tr(lang, "no_active_cancel"), business)
            return "ok", 200

        deleted_count = 0
        for r in reservations:
            event_id = r.get("google_event_id")
            if event_id:
                if delete_event(event_id, calendar_id="primary"):
                    deleted_count += 1

        cancelled_count = mark_reservations_cancelled_by_phone(business["id"], phone)

        send_message(
            phone,
            tr(lang, "cancel_done", count=cancelled_count, events=deleted_count),
            business,
        )
        return "ok", 200

    # STEP 1 – NAME
    if state and state.get("step") == "awaiting_name":
        lang = state.get("lang", lang)

        # User repeated a command instead of giving a name
        if is_booking_intent(t) or is_cancel_intent(t):
            send_message(phone, tr(lang, "ask_name"), business)
            return "ok", 200

        state["name"] = t
        state["step"] = "awaiting_service"

        send_message(
            phone,
            tr(lang, "ask_service", name=t),
            business,
        )
        return "ok", 200

    # STEP 2 – SERVICE (keywords + AI fallback)
    if state and state.get("step") == "awaiting_service":
        lang = state.get("lang", lang)
        lt2 = t.lower()

        # User repeated a command instead of giving a service
        if is_booking_intent(t) or is_cancel_intent(t):
            send_message(
                phone,
                tr(lang, "ask_service", name=state.get("name", "")),
                business,
            )
            return "ok", 200

        matched = set()
        for kw, canonical in SERVICE_KEYWORDS.items():
            if kw.lower() in lt2:
                matched.add(canonical)

        ai_service = None
        normalized = None

        if "Haircut" in matched and "Beard Trim" in matched:
            normalized = "Haircut and Beard"
        elif matched:
            normalized = next(iter(matched))
        else:
            if OPENROUTER_API_KEY:
                ai_service = ai_pick_service(business, t)
                if ai_service:
                    normalized = ai_service

        if normalized is None:
            normalized = t

        print("SERVICE STEP raw:", t, "ai:", ai_service, "normalized:", normalized)

        state["service"] = normalized
        state["step"] = "awaiting_date"

        send_message(
            phone,
            tr(lang, "ask_date", service=normalized),
            business,
        )
        return "ok", 200

    # STEP 3 – DATE
    if state and state.get("step") == "awaiting_date":
        lang = state.get("lang", lang)

        if is_booking_intent(t) or is_cancel_intent(t):
            send_message(
                phone,
                tr(lang, "ask_date", service=state.get("service", "")),
                business,
            )
            return "ok", 200

        try:
            normalized_date = normalize_booking_date(t)
        except Exception:
            send_message(phone, tr(lang, "invalid_date"), business)
            return "ok", 200

        day_rules = get_day_rules(business["id"], normalized_date)
        if day_rules.get("closed"):
            send_message(phone, tr(lang, "closed_day"), business)
            return "ok", 200

        state["date"] = normalized_date
        state["step"] = "awaiting_time"

        send_message(phone, tr(lang, "ask_time"), business)
        return "ok", 200

    # STEP 4 – TIME
    if state and state.get("step") == "awaiting_time":
        lang = state.get("lang", lang)

        if is_booking_intent(t) or is_cancel_intent(t):
            send_message(phone, tr(lang, "ask_time"), business)
            return "ok", 200

        time_ = normalize_time_str(t)

        if not time_:
            send_message(phone, tr(lang, "invalid_time"), business)
            return "ok", 200

        state["time"] = time_

        day_rules = get_day_rules(business["id"], state["date"])
        if day_rules.get("closed"):
            send_message(phone, tr(lang, "closed_day"), business)
            return "ok", 200

        if not is_time_within_business_hours(time_, day_rules["open_time"], day_rules["close_time"]):
            send_message(phone, tr(lang, "outside_hours"), business)
            return "ok", 200

        if is_slot_taken(business["id"], state["date"], time_):
            send_message(
                phone,
                tr(lang, "slot_taken", date=state["date"], time=time_),
                business,
            )
            return "ok", 200

        try:
            reservation_id = save_reservation(
                business["id"],
                phone,
                state.get("name", ""),
                state.get("service", ""),
                state.get("date", ""),
                state.get("time", ""),
            )
            print("Reservation saved with id:", reservation_id)

            gcal_event = add_reservation_to_google_calendar(
                state.get("name", ""),
                state.get("service", ""),
                state.get("date", ""),
                state.get("time", ""),
            )

            if gcal_event:
                event_id = gcal_event.get("id")
                print("Reservation added to Google Calendar:", event_id)
                if event_id:
                    save_google_event_id(reservation_id, event_id)
            else:
                print("Google Calendar event was not created")

            send_reservation_confirmation(
                phone,
                state.get("name", ""),
                state.get("service", ""),
                state.get("date", ""),
                state.get("time", ""),
                business,
                calendar_added=bool(gcal_event),
            )

            user_state.pop(key, None)
            return "ok", 200

        except Exception as e:
            print("STEP 4 save error:", str(e))
            send_message(phone, tr(lang, "save_error"), business)
            return "ok", 200



# ------------------ WEBHOOK ------------------

@app.route("/")
def home():
    return "OK", 200

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if mode == "subscribe" and token == VERIFY_TOKEN:
            print("WEBHOOK VERIFIED (Meta)")
            return challenge, 200
        return "Forbidden", 403

    raw = request.get_data(as_text=True)
    print("RAW META:", raw, flush=True)

    data = request.get_json(silent=True)
    print("INCOMING META:", data, flush=True)
    sys.stdout.flush()

    try:
        entry = data["entry"][0]
        change = entry["changes"][0]
        value = change["value"]
    except Exception as e:
        print("Meta webhook parse error:", e)
        return "ok", 200

    if "statuses" in value:
        return "ok", 200

    if "messages" not in value or not value["messages"]:
        return "ok", 200

    message = value["messages"][0]
    if message.get("type") != "text":
        return "ok", 200

    phone = message.get("from")
    text = message.get("text", {}).get("body", "").strip()
    phone_number_id = value["metadata"]["phone_number_id"]
    print("phone:", phone, "text:", text, "phone_number_id:", phone_number_id, flush=True)

    business = get_business_by_phone_number_id(phone_number_id)
    print("business lookup result:", dict(business) if business else None, flush=True)
    if not business:
        print("No business configured for phone_number_id", phone_number_id, flush=True)
        return "ok", 200

    print("Calling process_incoming_message...", flush=True)
    return process_incoming_message(business, phone, text)


# ------------------ SIMPLE DEBUG RESERVATIONS PAGE ------------------


@app.route("/reservations")
def reservations_page():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, customer_phone, customer_name, service, date, time, created_at
        FROM reservations
        ORDER BY id DESC
        """
    )
    rows = c.fetchall()
    conn.close()

    html = """
    <h2>Reservations</h2>
    <table border="1" cellpadding="6">
      <tr>
        <th>ID</th><th>Phone</th><th>Name</th>
        <th>Service</th><th>Date</th><th>Time</th><th>Created</th>
      </tr>
      {% for r in rows %}
        <tr>{% for v in r %}<td>{{v}}</td>{% endfor %}</tr>
      {% endfor %}
    </table>
    """
    return render_template_string(html, rows=rows)


# ------------------ ADMIN BUSINESSES ------------------ #


from flask import render_template_string, request  # make sure this is imported at the top

@app.route("/admin/businesses", methods=["GET", "POST"])
def admin_businesses():
    conn = get_db_connection()
    c = conn.cursor()

    if request.method == "POST":
        name            = request.form.get("name", "").strip()
        provider        = request.form.get("provider", "meta").strip().lower()
        phone_number_id = request.form.get("phone_number_id", "").strip()
        access_token    = request.form.get("access_token", "").strip()
        calendar_id     = request.form.get("calendar_id", "primary").strip()
        timezone        = request.form.get("timezone", "Asia/Beirut").strip()

        if name and phone_number_id and access_token:
            c.execute(
                """
                INSERT INTO businesses (name, provider, phone_number_id, access_token, calendar_id, timezone)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (phone_number_id) DO UPDATE SET
                  name = EXCLUDED.name,
                  provider = EXCLUDED.provider,
                  access_token = EXCLUDED.access_token,
                  calendar_id = EXCLUDED.calendar_id,
                  timezone = EXCLUDED.timezone
                """,
                (name, provider, phone_number_id, access_token, calendar_id, timezone),
            )
            conn.commit()

    # Load all businesses
    c.execute(
        "SELECT id, name, provider, phone_number_id, timezone FROM businesses ORDER BY id"
    )
    businesses = c.fetchall()
    conn.close()

    html = """
    <h1>Admin — Businesses</h1>

    <h2>Existing businesses</h2>
    {% if businesses %}
    <table border="1" cellpadding="6">
      <tr>
        <th>ID</th>
        <th>Name</th>
        <th>Provider</th>
        <th>Phone Number ID</th>
        <th>Timezone</th>
        <th>Services</th>
        <th>Dashboard</th>
      </tr>
      {% for b in businesses %}
      <tr>
        <td>{{ b.id }}</td>
        <td>{{ b.name }}</td>
        <td>{{ b.provider }}</td>
        <td>{{ b.phone_number_id }}</td>
        <td>{{ b.timezone }}</td>
        <td><a href="/admin/{{ b.id }}/services">Manage services</a></td>
        <!-- 🔥 This link is EXACTLY the one that works when you paste it -->
        <td><a href="/dashboard?business_id={{ b.id }}">Open dashboard</a></td>
      </tr>
      {% endfor %}
    </table>
    {% else %}
    <p>No businesses yet.</p>
    {% endif %}

    <h2 style="margin-top:30px;">Add new business</h2>
    <form method="post">
      <p>Name: <input name="name" required></p>

      <p>
        Provider:
        <select name="provider">
          <option value="meta">Meta Cloud API</option>
          <option value="360dialog">360dialog</option>
        </select>
      </p>

      <p>Phone Number ID (Meta only): <input name="phone_number_id"></p>
      <p>Access Token (Meta only): <input name="access_token" style="width:400px;"></p>

      <p>Calendar ID: <input name="calendar_id" value="primary"></p>
      <p>Timezone: <input name="timezone" value="Asia/Beirut"></p>

      <p><button type="submit">Add business</button></p>
    </form>
    """
    return render_template_string(html, businesses=businesses)


# ------------------ ADMIN SERVICES ------------------


@app.route("/admin/<int:business_id>/services", methods=["GET", "POST"])
def admin_services(business_id):
    conn = get_db_connection()
    c = conn.cursor()

    # Make sure business exists
    c.execute("SELECT name FROM businesses WHERE id=%s", (business_id,))
    biz = c.fetchone()
    if not biz:
        conn.close()
        return f"No business with ID {business_id}", 404

    business_name = biz["name"]

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        price = request.form.get("price", "").strip()
        dur = request.form.get("duration_min", "").strip()

        try:
            price_f = float(price) if price else 0.0
        except ValueError:
            price_f = 0.0

        try:
            dur_i = int(dur) if dur else 30
        except ValueError:
            dur_i = 30

        if name:
            c.execute(
                """
                INSERT INTO services (name, price, duration_min, business_id)
                VALUES (%s, %s, %s, %s)
                """,
                (name, price_f, dur_i, business_id),
            )
            conn.commit()

    # List services for this business
    c.execute(
        """
        SELECT id, name, price, duration_min
        FROM services
        WHERE business_id=%s
        ORDER BY id
        """,
        (business_id,),
    )
    services = c.fetchall()
    conn.close()

    html = """
    <h1>Services — {{ business_name }}</h1>

    <table border="1" cellpadding="6">
      <tr><th>ID</th><th>Name</th><th>Price</th><th>Duration (min)</th></tr>
      {% for s in services %}
      <tr>
        <td>{{ s.id }}</td>
        <td>{{ s.name }}</td>
        <td>{{ s.price }}</td>
        <td>{{ s.duration_min }}</td>
      </tr>
      {% endfor %}
    </table>

    <h2 style="margin-top:30px;">Add service</h2>
    <form method="post">
      <p>Name: <input name="name" required></p>
      <p>Price: <input name="price" value="0"></p>
      <p>Duration (min): <input name="duration_min" value="30"></p>
      <p><button type="submit">Add</button></p>
    </form>

    <p style="margin-top:20px;">
      <a href="/dashboard %s business_id={{ business_id }}">Back to dashboard</a> |
      <a href="/admin/businesses">Back to businesses</a>
    </p>
    """
    return render_template_string(
        html,
        services=services,
        business_name=business_name,
        business_id=business_id,
    )


# ------------------ AUTH: REGISTER / LOGIN / LOGOUT ------------------


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()

        conn = get_db_connection()
        c = conn.cursor()

        # pick first business by default
        c.execute("SELECT id FROM businesses ORDER BY id LIMIT 1")
        row = c.fetchone()
        if not row:
            conn.close()
            return "No business exists yet. Create one in /admin/businesses first.", 400

        business_id = row[0]
        pw_hash = generate_password_hash(password)

        try:
            c.execute(
                "INSERT INTO users(business_id, email, password_hash) VALUES (%s, %s, %s)",
                (business_id, email, pw_hash),
            )
            conn.commit()
        except Exception as e:
            conn.close()
            return f"Error creating user (maybe email already used): {e}", 400

        conn.close()
        return redirect("/login")

    return """
    <h3>Register dashboard user</h3>
    <p>(Will be attached to the first business in DB)</p>
    <form method="POST">
        Email:<br><input name="email"><br>
        Password:<br><input name="password" type="password"><br><br>
        <button>Register</button>
    </form>
    """


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()

        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "SELECT id, business_id, password_hash FROM users WHERE email=%s", (email,)
        )
        user = c.fetchone()
        conn.close()

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["business_id"] = user["business_id"]
            return redirect("/dashboard")

        return "Invalid login", 403

    return """
    <h3>Login</h3>
    <form method="POST">
        Email:<br><input name="email"><br>
        Password:<br><input name="password" type="password"><br><br>
        <button>Login</button>
    </form>
    """


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


# ------------------ DASHBOARD + CONFIRM / CANCEL ------------------


@app.route("/dashboard")
def dashboard():
    business_id = session.get("business_id") or request.args.get("business_id", type=int)

    conn = get_db_connection()
    c = conn.cursor()

    if business_id is None:
        c.execute("SELECT * FROM businesses ORDER BY id LIMIT 1")
        business = c.fetchone()
        if not business:
            conn.close()
            return redirect("/admin/businesses")
    else:
        c.execute("SELECT * FROM businesses WHERE id = %s", (business_id,))
        business = c.fetchone()
        if not business:
            conn.close()
            return f"No business with ID {business_id}", 404

    business_id = business["id"]
    session["business_id"] = business_id

    ensure_default_hours(business_id)

    c.execute(
        """
        SELECT id, customer_name, customer_phone, service, date, time, status
        FROM reservations
        WHERE business_id = %s
        ORDER BY date, time
        """,
        (business_id,),
    )
    reservations = c.fetchall()

    c.execute(
        """
        SELECT id, name, price, duration_min
        FROM services
        WHERE business_id = %s
        ORDER BY id DESC
        """,
        (business_id,),
    )
    services = c.fetchall()

    c.execute(
        """
        SELECT id, weekday, is_closed,
               TO_CHAR(open_time, 'HH24:MI') AS open_time,
               TO_CHAR(close_time, 'HH24:MI') AS close_time
        FROM business_hours
        WHERE business_id = %s
        ORDER BY weekday
        """,
        (business_id,),
    )
    hours = c.fetchall()

    c.execute(
        """
        SELECT id, blocked_date::text AS blocked_date, COALESCE(note, '') AS note
        FROM blocked_dates
        WHERE business_id = %s
        ORDER BY blocked_date
        """,
        (business_id,),
    )
    blocked_dates = c.fetchall()

    conn.close()

    return render_template(
        "dashboard.html",
        business=business,
        reservations=reservations,
        services=services,
        hours=hours,
        blocked_dates=blocked_dates,
        weekday_names=WEEKDAY_NAMES,
        active_tab=request.args.get("tab", "reservations"),
        google_calendar_connected=bool(business.get("gcal_credentials")),
        whatsapp_connected=bool(business.get("access_token")),
    )



@app.route("/confirm/<int:reservation_id>")
def confirm_reservation(reservation_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT customer_phone, customer_name, service, date, time, status
        FROM reservations
        WHERE id = %s AND business_id = %s
        """,
        (reservation_id, business_id),
    )
    row = c.fetchone()

    if not row:
        conn.close()
        return redirect("/dashboard")

    phone, name, service, date, time, status = row

    # If it's already confirmed (bot did it), just redirect
    if status == "CONFIRMED":
        conn.close()
        return redirect("/dashboard")

    # Otherwise, mark it as confirmed silently (no extra WhatsApp message)
    c.execute(
        """
        UPDATE reservations
        SET status = 'CONFIRMED'
        WHERE id = %s AND business_id = %s
        """,
        (reservation_id, business_id),
    )
    conn.commit()
    conn.close()

    # No second send_reservation_confirmation here
    return redirect("/dashboard")




@app.route("/cancel/<int:reservation_id>")
def cancel_reservation(reservation_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT customer_phone, customer_name, service, date, time, google_event_id
        FROM reservations
        WHERE id = %s AND business_id = %s
        """,
        (reservation_id, business_id),
    )
    row = c.fetchone()

    if not row:
        conn.close()
        return redirect("/dashboard")

    phone = row["customer_phone"]
    name = row["customer_name"]
    service = row["service"]
    date = row["date"]
    time = row["time"]
    google_event_id = row.get("google_event_id")

    business = get_business_by_id(business_id)

    # Delete Google Calendar event first
    if business and google_event_id:
        try:
            deleted = delete_event(google_event_id, calendar_id="primary")
            print("Google Calendar delete result:", deleted)
        except Exception as e:
            print("Error deleting Google Calendar event:", e)

    c.execute(
        """
        UPDATE reservations
        SET status = 'CANCELED'
        WHERE id = %s AND business_id = %s
        """,
        (reservation_id, business_id),
    )
    conn.commit()
    conn.close()

    if business:
        try:
            send_reservation_cancellation(phone, name, service, date, time, business)
        except Exception as e:
            print("Error sending WhatsApp cancellation:", e)

    return redirect("/dashboard")

@app.route("/temp/list-tables", methods=["GET"])
def temp_list_tables():
    try:
        admin_key = request.headers.get("X-Admin-Key", "")
        expected_key = os.getenv("TEMP_ADMIN_KEY", "")

        if not expected_key or admin_key != expected_key:
            return jsonify({"ok": False, "error": "unauthorized"}), 403

        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            return jsonify({"ok": False, "error": "DATABASE_URL is missing"}), 500

        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)

        tables = [row[0] for row in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({"ok": True, "tables": tables}), 200

    except Exception as e:
        print("temp_list_tables error:", str(e))
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/temp/table-columns/<table_name>", methods=["GET"])
def temp_table_columns(table_name):
    try:
        admin_key = request.headers.get("X-Admin-Key", "")
        expected_key = os.getenv("TEMP_ADMIN_KEY", "")

        if not expected_key or admin_key != expected_key:
            return jsonify({"ok": False, "error": "unauthorized"}), 403

        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            return jsonify({"ok": False, "error": "DATABASE_URL is missing"}), 500

        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()

        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))

        columns = [{"name": row[0], "type": row[1]} for row in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({"ok": True, "table": table_name, "columns": columns}), 200

    except Exception as e:
        print("temp_table_columns error:", str(e))
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/temp/view-reservations", methods=["GET"])
def temp_view_reservations():
    try:
        admin_key = request.headers.get("X-Admin-Key", "")
        expected_key = os.getenv("TEMP_ADMIN_KEY", "")

        if not expected_key or admin_key != expected_key:
            return jsonify({"ok": False, "error": "unauthorized"}), 403

        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            return jsonify({"ok": False, "error": "DATABASE_URL is missing"}), 500

        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()

        cur.execute("SELECT * FROM reservations ORDER BY id DESC LIMIT 50")
        rows = cur.fetchall()

        column_names = [desc[0] for desc in cur.description]

        reservations = []
        for row in rows:
            item = {}
            for i, value in enumerate(row):
                item[column_names[i]] = str(value) if value is not None else None
            reservations.append(item)

        cur.close()
        conn.close()

        return jsonify({
            "ok": True,
            "count": len(reservations),
            "reservations": reservations
        }), 200

    except Exception as e:
        print("temp_view_reservations error:", str(e))
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/settings/update", methods=["POST"])
def update_business_settings():
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    business_name = request.form.get("business_name", "").strip()
    timezone = request.form.get("timezone", "Asia/Beirut").strip() or "Asia/Beirut"

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        UPDATE businesses
        SET name = %s, timezone = %s
        WHERE id = %s
        """,
        (business_name, timezone, business_id),
    )
    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=settings")


@app.route("/services/add", methods=["POST"])
def add_service():
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    name = request.form.get("name", "").strip()
    price = float(request.form.get("price") or 0)
    duration_min = int(request.form.get("duration_min") or 45)

    if name:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO services (name, price, duration_min, business_id)
            VALUES (%s, %s, %s, %s)
            """,
            (name, price, duration_min, business_id),
        )
        conn.commit()
        conn.close()

    return redirect("/dashboard?tab=services")


@app.route("/services/delete/<int:service_id>", methods=["POST"])
def delete_service(service_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        DELETE FROM services
        WHERE id = %s AND business_id = %s
        """,
        (service_id, business_id),
    )
    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=services")


@app.route("/availability/update-hours", methods=["POST"])
def update_hours():
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()

    for weekday in range(7):
        is_closed = request.form.get(f"closed_{weekday}") == "on"
        open_time = request.form.get(f"open_{weekday}") or None
        close_time = request.form.get(f"close_{weekday}") or None

        if is_closed:
            open_time = None
            close_time = None

        c.execute(
            """
            INSERT INTO business_hours (business_id, weekday, is_closed, open_time, close_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (business_id, weekday)
            DO UPDATE SET
                is_closed = EXCLUDED.is_closed,
                open_time = EXCLUDED.open_time,
                close_time = EXCLUDED.close_time
            """,
            (business_id, weekday, is_closed, open_time, close_time),
        )

    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=settings")


@app.route("/availability/add-blocked-date", methods=["POST"])
def add_blocked_date():
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    blocked_date_raw = request.form.get("blocked_date", "").strip()
    note = request.form.get("note", "").strip()

    if blocked_date_raw:
        blocked_date = normalize_booking_date(blocked_date_raw)

        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO blocked_dates (business_id, blocked_date, note)
            VALUES (%s, %s, %s)
            ON CONFLICT (business_id, blocked_date)
            DO UPDATE SET note = EXCLUDED.note
            """,
            (business_id, blocked_date, note),
        )
        conn.commit()
        conn.close()

    return redirect("/dashboard?tab=settings")


@app.route("/availability/delete-blocked-date/<int:block_id>", methods=["POST"])
def delete_blocked_date(block_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        DELETE FROM blocked_dates
        WHERE id = %s AND business_id = %s
        """,
        (block_id, business_id),
    )
    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=settings")


# ------------------ RUN ------------------

if __name__ == "__main__":
    init_db()       # <-- creates tables automatically
    app.run(host="0.0.0.0", port=10000)




