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
from dateutil import parser as dtparse
import pytz
import sys
import time
from urllib.parse import quote_plus
import secrets
from flask import abort
# ------------------ BUSINESS HELPERS ------------------

processed_message_ids = {}
processing_message_ids = {}

PROCESSED_MESSAGE_TTL = 60 * 60   # 1 hour
PROCESSING_MESSAGE_TTL = 60        # 1 minute


def cleanup_message_tracking():
    now = time.time()

    expired_done = [
        mid for mid, ts in processed_message_ids.items()
        if now - ts > PROCESSED_MESSAGE_TTL
    ]
    for mid in expired_done:
        processed_message_ids.pop(mid, None)

    expired_processing = [
        mid for mid, ts in processing_message_ids.items()
        if now - ts > PROCESSING_MESSAGE_TTL
    ]
    for mid in expired_processing:
        processing_message_ids.pop(mid, None)


def is_message_already_done(message_id):
    cleanup_message_tracking()
    return bool(message_id and message_id in processed_message_ids)


def is_message_currently_processing(message_id):
    cleanup_message_tracking()
    return bool(message_id and message_id in processing_message_ids)


def mark_message_processing(message_id):
    if message_id:
        processing_message_ids[message_id] = time.time()


def mark_message_done(message_id):
    if message_id:
        processing_message_ids.pop(message_id, None)
        processed_message_ids[message_id] = time.time()


def clear_message_processing(message_id):
    if message_id:
        processing_message_ids.pop(message_id, None)

def get_business_by_phone_number_id(phone_number_id: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM businesses WHERE phone_number_id=%s LIMIT 1",
        (phone_number_id,),
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


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
app.secret_key = os.getenv("SECRET_KEY", "change-me-in-render")

def get_csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token

app.jinja_env.globals["csrf_token"] = get_csrf_token

@app.before_request
def csrf_protect():
    if request.method != "POST":
        return

    # Skip routes that should not use form CSRF
    if request.path == "/webhook":
        return
    if request.path.startswith("/login"):
        return
    if request.path.startswith("/register"):
        return
    if request.path.startswith("/admin/"):
        return

    session_token = session.get("_csrf_token", "")
    form_token = request.form.get("_csrf_token", "")

    if not session_token or not form_token or session_token != form_token:
        abort(403)


_fb_tables_ready = False

def ensure_fb_tables():
    global _fb_tables_ready
    if _fb_tables_ready:
        return

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS fb_products (
            id SERIAL PRIMARY KEY,
            business_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            price NUMERIC(10,2) NOT NULL DEFAULT 0,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS fb_sales (
            id SERIAL PRIMARY KEY,
            business_id INTEGER NOT NULL,
            total_amount NUMERIC(10,2) NOT NULL DEFAULT 0,
            sold_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS fb_sale_items (
            id SERIAL PRIMARY KEY,
            sale_id INTEGER NOT NULL REFERENCES fb_sales(id) ON DELETE CASCADE,
            product_id INTEGER,
            product_name_snapshot TEXT NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            unit_price NUMERIC(10,2) NOT NULL DEFAULT 0,
            line_total NUMERIC(10,2) NOT NULL DEFAULT 0
        )
        """
    )

    c.execute("CREATE INDEX IF NOT EXISTS idx_fb_products_business ON fb_products (business_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_fb_sales_business_sold_at ON fb_sales (business_id, sold_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_fb_sale_items_sale_id ON fb_sale_items (sale_id)")

    conn.commit()
    conn.close()
    _fb_tables_ready = True


_business_feature_columns_ready = False
_reservation_extension_columns_ready = False

def ensure_business_feature_columns():
    global _business_feature_columns_ready
    if _business_feature_columns_ready:
        return

    conn = get_db_connection()
    c = conn.cursor()
    c.execute("ALTER TABLE businesses ADD COLUMN IF NOT EXISTS enable_fb BOOLEAN")
    c.execute("ALTER TABLE businesses ADD COLUMN IF NOT EXISTS enable_resource_blocking BOOLEAN")
    c.execute("ALTER TABLE businesses ADD COLUMN IF NOT EXISTS enable_time_extension BOOLEAN")
    c.execute("ALTER TABLE businesses ADD COLUMN IF NOT EXISTS extension_pricing_mode TEXT")
    c.execute("ALTER TABLE businesses ADD COLUMN IF NOT EXISTS extension_flat_30_price NUMERIC(10,2)")
    conn.commit()
    conn.close()

    _business_feature_columns_ready = True


def ensure_reservation_extension_columns():
    global _reservation_extension_columns_ready
    if _reservation_extension_columns_ready:
        return

    conn = get_db_connection()
    c = conn.cursor()
    c.execute("ALTER TABLE reservations ADD COLUMN IF NOT EXISTS extra_minutes INTEGER NOT NULL DEFAULT 0")
    c.execute("ALTER TABLE reservations ADD COLUMN IF NOT EXISTS extra_price NUMERIC(10,2) NOT NULL DEFAULT 0")
    conn.commit()
    conn.close()

    _reservation_extension_columns_ready = True


_service_metadata_columns_ready = False

def ensure_service_metadata_columns():
    global _service_metadata_columns_ready
    if _service_metadata_columns_ready:
        return

    conn = get_db_connection()
    c = conn.cursor()
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS sport_category TEXT")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS night_price NUMERIC(10,2)")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS capacity_units_used INTEGER")
    conn.commit()
    conn.close()
    _service_metadata_columns_ready = True


SPECIAL_NIGHT_PRICE_MAP = {
    "basketball half court 1 hour": 20.0,
    "basketball half court 2 hour": 40.0,
    "basketball full court 1 hour": 35.0,
    "basketball full court 2 hour": 70.0,
    "tennis full court 1 hour": 15.0,
    "tennis full court 2 hour": 40.0,
}

SPORT_ALIASES = {
    "basketball": {"basketball", "basket ball", "basket"},
    "tennis": {"tennis"},
    "padel": {"padel"},
}

def is_night_time_str(time_str, threshold="19:00"):
    try:
        normalized = normalize_time_str(time_str) or str(time_str)
        return time_to_minutes(normalized) >= time_to_minutes(threshold)
    except Exception:
        return False

def infer_service_sport_from_name(service_name):
    name = (service_name or "").strip().lower()
    for sport, aliases in SPORT_ALIASES.items():
        if any(alias in name for alias in aliases):
            return sport
    return None

def infer_service_capacity_units_from_name(service_name):
    name = (service_name or "").strip().lower()
    if "full court" in name:
        return 2
    if "half court" in name:
        return 1
    return 1

def get_service_metadata_row(business_id, service_name):
    ensure_service_metadata_columns()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, name, price, duration_min, sport_category, night_price, capacity_units_used
        FROM services
        WHERE business_id = %s
          AND lower(trim(name)) = lower(trim(%s))
        LIMIT 1
        """,
        (business_id, service_name),
    )
    row = c.fetchone()
    if not row:
        c.execute(
            """
            SELECT id, name, price, duration_min, sport_category, night_price, capacity_units_used
            FROM services
            WHERE business_id = %s
              AND lower(name) LIKE lower(%s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (business_id, f"%{service_name.strip()}%"),
        )
        row = c.fetchone()
    conn.close()
    return row

def get_service_sport_category(business_id, service_name):
    row = get_service_metadata_row(business_id, service_name)
    value = (row.get("sport_category") or "").strip().lower() if row else ""
    return value or infer_service_sport_from_name(service_name)

def get_service_capacity_units(business_id, service_name):
    row = get_service_metadata_row(business_id, service_name)
    value = safe_int(row.get("capacity_units_used") if row else None, 0)
    return value if value > 0 else infer_service_capacity_units_from_name(service_name)

def get_service_night_price(business_id, service_name):
    row = get_service_metadata_row(business_id, service_name)
    if row and row.get("night_price") is not None:
        return safe_float(row.get("night_price"), 0.0)
    return SPECIAL_NIGHT_PRICE_MAP.get((service_name or "").strip().lower())

def get_effective_service_price(business_id, service_name, time_str=None):
    base_price = float(get_service_info(business_id, service_name).get("price", 0) or 0)
    if time_str and is_night_time_str(time_str):
        night_price = get_service_night_price(business_id, service_name)
        if night_price is not None:
            return float(night_price)
    return base_price

def get_effective_service_price_for_reservation_row(business, reservation):
    return get_effective_service_price(business["id"], reservation.get("service"), reservation.get("time"))

def get_service_shared_pool_key(business_id, service_name):
    sport = get_service_sport_category(business_id, service_name)
    if sport in {"basketball", "tennis"}:
        return "shared_tennis_basketball_court"
    return None

def get_service_pool_capacity(business_id, resource, service_name):
    if get_service_shared_pool_key(business_id, service_name):
        return 2
    return int((resource or {}).get("capacity") or 1)

def get_available_sports_for_business(business_id):
    ensure_service_metadata_columns()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT name, sport_category FROM services WHERE business_id = %s ORDER BY id DESC", (business_id,))
    rows = c.fetchall()
    conn.close()
    sports = []
    seen = set()
    for row in rows:
        sport = ((row.get("sport_category") or "").strip().lower() or infer_service_sport_from_name(row.get("name")))
        if sport and sport not in seen:
            seen.add(sport)
            sports.append(sport)
    return sports

def should_use_sport_first_flow(business_id):
    sports = get_available_sports_for_business(business_id)
    return len([s for s in sports if s in {"padel", "basketball", "tennis"}]) >= 2

def format_service_bullets_for_sport(business_id, sport):
    services = []
    for s in get_service_names_for_business(business_id):
        if (get_service_sport_category(business_id, s) or "").lower() == (sport or "").lower():
            services.append(s)
    if not services:
        return "• No services configured yet"
    return "\n".join([f"• {s}" for s in services])

def resolve_valid_service_and_sport(business_id, text, selected_sport=None):
    valid_service, available_services = validate_service_for_business(business_id, text)
    if not valid_service:
        return None, None, available_services
    sport = get_service_sport_category(business_id, valid_service)
    if selected_sport and sport and sport.lower() != selected_sport.lower():
        return None, sport, available_services
    return valid_service, sport, available_services


_resource_availability_tables_ready = False

def ensure_resource_availability_tables():
    global _resource_availability_tables_ready
    if _resource_availability_tables_ready:
        return

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_hours (
            id SERIAL PRIMARY KEY,
            business_id INTEGER NOT NULL,
            resource_id INTEGER NOT NULL,
            weekday INTEGER NOT NULL,
            is_closed BOOLEAN NOT NULL DEFAULT FALSE,
            open_time TIME,
            close_time TIME
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS resource_blocked_dates (
            id SERIAL PRIMARY KEY,
            business_id INTEGER NOT NULL,
            resource_id INTEGER NOT NULL,
            blocked_date DATE NOT NULL,
            note TEXT
        )
        """
    )
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_hours_unique ON resource_hours (business_id, resource_id, weekday)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_blocked_unique ON resource_blocked_dates (business_id, resource_id, blocked_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_resource_blocked_lookup ON resource_blocked_dates (business_id, resource_id, blocked_date)")
    conn.commit()
    conn.close()

    _resource_availability_tables_ready = True

def infer_business_feature_defaults(business_id):
    ensure_fb_tables()

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT DISTINCT lower(coalesce(resource_type, '')) AS resource_type
        FROM resources
        WHERE business_id = %s
        """,
        (business_id,),
    )
    resource_types = {row["resource_type"] for row in c.fetchall() if row.get("resource_type")}
    has_court_like_resources = bool(resource_types.intersection({"court", "room", "pool"}))

    c.execute("SELECT 1 FROM fb_products WHERE business_id = %s LIMIT 1", (business_id,))
    has_fb_products = bool(c.fetchone())

    c.execute("SELECT 1 FROM fb_sales WHERE business_id = %s LIMIT 1", (business_id,))
    has_fb_sales = bool(c.fetchone())

    conn.close()

    return {
        "enable_fb": has_fb_products or has_fb_sales,
        "enable_resource_blocking": has_court_like_resources,
        "enable_time_extension": has_court_like_resources,
    }


def get_business_feature_flags(business):
    ensure_business_feature_columns()
    ensure_fb_tables()
    business_id = business["id"]

    inferred = infer_business_feature_defaults(business_id)

    def resolve_boolean(field_name):
        value = business.get(field_name)
        if value is None:
            return inferred[field_name]
        return bool(value)

    pricing_mode = (business.get("extension_pricing_mode") or "").strip().lower() or "tier_diff"
    flat_30_price = safe_float(business.get("extension_flat_30_price") or 0, 0.0)

    return {
        "enable_fb": resolve_boolean("enable_fb"),
        "enable_resource_blocking": resolve_boolean("enable_resource_blocking"),
        "enable_time_extension": resolve_boolean("enable_time_extension"),
        "extension_pricing_mode": pricing_mode,
        "extension_flat_30_price": flat_30_price,
    }


def business_has_feature(business, feature_name):
    return bool(get_business_feature_flags(business).get(feature_name))


def get_service_price_for_duration(business_id, duration_min):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT price
        FROM services
        WHERE business_id = %s
          AND duration_min = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (business_id, duration_min),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return float(row.get("price") or 0)


def get_reservation_base_duration_minutes(business_id, service_name):
    return int(get_service_info(business_id, service_name).get("duration", 45))


def get_reservation_total_duration_minutes(business_id, service_name, extra_minutes=0):
    return get_reservation_base_duration_minutes(business_id, service_name) + int(extra_minutes or 0)


def get_reservation_total_price(business_id, service_name, extra_price=0):
    base_price = float(get_service_info(business_id, service_name).get("price", 0) or 0)
    return base_price + float(extra_price or 0)


def calculate_extension_extra_charge(business, service_name, current_extra_minutes=0, increment_minutes=30):
    flags = get_business_feature_flags(business)
    business_id = business["id"]

    base_price = float(get_service_info(business_id, service_name).get("price", 0) or 0)
    current_extra_minutes = int(current_extra_minutes or 0)
    current_total_price = get_reservation_total_price(business_id, service_name, 0) + float(0)
    current_total_price = base_price + 0  # explicit for readability

    # What is already being charged for the current reservation state?
    current_reservation_price = base_price
    if current_extra_minutes:
        current_duration = get_reservation_base_duration_minutes(business_id, service_name) + current_extra_minutes
        current_duration_price = get_service_price_for_duration(business_id, current_duration)
        if current_duration_price is not None:
            current_reservation_price = current_duration_price
        else:
            current_reservation_price = base_price + (safe_float(flags.get("extension_flat_30_price") or 0) * (current_extra_minutes // 30))
    else:
        current_duration = get_reservation_base_duration_minutes(business_id, service_name)

    new_duration = current_duration + int(increment_minutes or 0)
    new_duration_price = get_service_price_for_duration(business_id, new_duration)

    if flags.get("extension_pricing_mode") == "flat_30":
        flat = safe_float(flags.get("extension_flat_30_price") or 0, 0.0)
        if flat > 0:
            return flat

    if new_duration_price is not None:
        extra = round(new_duration_price - current_reservation_price, 2)
        return max(0.0, extra)

    flat = safe_float(flags.get("extension_flat_30_price") or 0, 0.0)
    if flat > 0:
        return flat

    return None


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return int(default)


def get_fb_products(business_id, active_only=False):
    ensure_fb_tables()
    conn = get_db_connection()
    c = conn.cursor()
    if active_only:
        c.execute(
            """
            SELECT id, business_id, name, price, is_active, created_at, updated_at
            FROM fb_products
            WHERE business_id = %s AND is_active = TRUE
            ORDER BY id ASC
            """,
            (business_id,),
        )
    else:
        c.execute(
            """
            SELECT id, business_id, name, price, is_active, created_at, updated_at
            FROM fb_products
            WHERE business_id = %s
            ORDER BY id ASC
            """,
            (business_id,),
        )
    rows = c.fetchall()
    conn.close()
    return rows


def get_fb_recent_sales(business, limit=10):
    ensure_fb_tables()
    business_id = business["id"]
    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, total_amount, sold_at
        FROM fb_sales
        WHERE business_id = %s
        ORDER BY sold_at DESC, id DESC
        LIMIT %s
        """,
        (business_id, limit),
    )
    sales = c.fetchall()

    sale_ids = [row["id"] for row in sales]
    items_by_sale = {}
    if sale_ids:
        c.execute(
            """
            SELECT sale_id, product_name_snapshot, quantity
            FROM fb_sale_items
            WHERE sale_id = ANY(%s)
            ORDER BY id ASC
            """,
            (sale_ids,),
        )
        for row in c.fetchall():
            items_by_sale.setdefault(row["sale_id"], []).append(
                f'{row["quantity"]}x {row["product_name_snapshot"]}'
            )

    conn.close()

    results = []
    for sale in sales:
        sold_at = sale.get("sold_at")
        sold_at_display = "-"
        if sold_at:
            try:
                if sold_at.tzinfo is None:
                    sold_at = pytz.utc.localize(sold_at)
                sold_at_display = sold_at.astimezone(tz).strftime("%d %b %H:%M")
            except Exception:
                try:
                    sold_at_display = sold_at.strftime("%d %b %H:%M")
                except Exception:
                    sold_at_display = str(sold_at)

        results.append(
            {
                "id": sale["id"],
                "total_amount": float(sale.get("total_amount") or 0),
                "sold_at_display": sold_at_display,
                "items_summary": ", ".join(items_by_sale.get(sale["id"], [])) or "-",
            }
        )

    return results


def compute_fb_report_metrics(business):
    ensure_fb_tables()
    business_id = business["id"]
    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")
    today = datetime.now(tz).date()
    current_start = today - timedelta(days=6)
    previous_start = today - timedelta(days=13)
    previous_end = today - timedelta(days=7)

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT id, total_amount, sold_at
        FROM fb_sales
        WHERE business_id = %s
        ORDER BY sold_at DESC, id DESC
        """,
        (business_id,),
    )
    sales = c.fetchall()

    c.execute(
        """
        SELECT
            s.sold_at,
            i.product_name_snapshot,
            i.quantity,
            i.line_total
        FROM fb_sale_items i
        JOIN fb_sales s ON s.id = i.sale_id
        WHERE s.business_id = %s
        ORDER BY s.sold_at DESC, i.id DESC
        """,
        (business_id,),
    )
    items = c.fetchall()
    conn.close()

    weekly_fb_revenue = 0.0
    previous_weekly_fb_revenue = 0.0
    fb_total_revenue = 0.0

    for sale in sales:
        sold_at = sale.get("sold_at")
        if not sold_at:
            continue

        try:
            if sold_at.tzinfo is None:
                sold_at_local = pytz.utc.localize(sold_at).astimezone(tz)
            else:
                sold_at_local = sold_at.astimezone(tz)
        except Exception:
            sold_at_local = sold_at

        sold_date = sold_at_local.date()
        amount = float(sale.get("total_amount") or 0)
        fb_total_revenue += amount

        if current_start <= sold_date <= today:
            weekly_fb_revenue += amount
        elif previous_start <= sold_date <= previous_end:
            previous_weekly_fb_revenue += amount

    weekly_item_map = {}
    for row in items:
        sold_at = row.get("sold_at")
        if not sold_at:
            continue
        try:
            if sold_at.tzinfo is None:
                sold_at_local = pytz.utc.localize(sold_at).astimezone(tz)
            else:
                sold_at_local = sold_at.astimezone(tz)
        except Exception:
            sold_at_local = sold_at

        sold_date = sold_at_local.date()
        if not (current_start <= sold_date <= today):
            continue

        name = (row.get("product_name_snapshot") or "Unnamed item").strip()
        weekly_item_map.setdefault(name, {"name": name, "quantity": 0, "revenue": 0.0})
        weekly_item_map[name]["quantity"] += int(row.get("quantity") or 0)
        weekly_item_map[name]["revenue"] += float(row.get("line_total") or 0)

    weekly_items = sorted(
        weekly_item_map.values(),
        key=lambda x: (-x["revenue"], x["name"].lower()),
    )

    return {
        "weekly_fb_revenue": weekly_fb_revenue,
        "previous_weekly_fb_revenue": previous_weekly_fb_revenue,
        "fb_total_revenue": fb_total_revenue,
        "weekly_fb_items": weekly_items,
    }


# ------------------ CONFIG (.env) ------------------

load_dotenv()

OPENROUTER_API_KEY = (os.getenv("OPENROUTER_API_KEY") or "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
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
    # Lebanese slang / transliteration
    "oss cha3re": "Haircut",
    "قص شعري": "Haircut",
    "قص شعريي": "Haircut",
    "قص شعري": "Haircut",
    "cha3r": "Haircut",
    "sha3r": "Haircut",
    "oss": "Haircut",
    "d2n": "Beard Trim",
    "da2n": "Beard Trim",
    "lehye": "Beard Trim",
}


# ------------------ LOW-LEVEL HELPERS ------------------

def send_message(to: str, text: str, business: dict):
    if not business.get("phone_number_id") or not business.get("access_token"):
        print("send_message: missing phone_number_id or access_token for business")
        return False

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
        print("send_message status:", r.status_code, flush=True)
        print("send_message body:", r.text, flush=True)
        return r.ok
    except Exception as e:
        print("send_message error (meta):", e)
        return False

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
        SELECT name, price, duration_min
        FROM services
        WHERE business_id = %s
          AND lower(trim(name)) = lower(trim(%s))
        LIMIT 1
        """,
        (business_id, service_name),
    )
    row = c.fetchone()

    if not row:
        c.execute(
            """
            SELECT name, price, duration_min
            FROM services
            WHERE business_id = %s
              AND lower(name) LIKE lower(%s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (business_id, f"%{service_name.strip()}%"),
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
    c.execute("SELECT name FROM services WHERE business_id = %s ORDER BY id", (business_id,))
    rows = c.fetchall()
    conn.close()
    return [r["name"] for r in rows]


def format_service_list(business_id):
    services = get_service_names_for_business(business_id)
    if not services:
        return ""
    return ", ".join(services)

def format_service_bullets_for_business(business_id):
    services = get_service_names_for_business(business_id)
    if not services:
        return "• No services configured yet"
    return "\n".join([f"• {s}" for s in services])

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
    business_id,
    date_iso,
    requested_time_str,
    service_name,
    open_start="09:00",
    open_end="18:00",
    step_min=15,
    max_suggestions=3,
):
    requested_norm = normalize_time_str_with_hours(
        requested_time_str,
        open_start,
        open_end,
    ) or open_start

    requested_minutes = time_to_minutes(requested_norm)
    open_minutes = time_to_minutes(open_start)
    close_minutes = time_to_minutes(open_end)

    new_duration = int(get_service_info(business_id, service_name).get("duration", 45))

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT service, time
        FROM reservations
        WHERE business_id = %s
          AND date = %s
          AND status = 'CONFIRMED'
        """,
        (business_id, date_iso),
    )
    rows = c.fetchall()
    conn.close()

    existing_intervals = []
    for row in rows:
        existing_time = normalize_time_str(row["time"])
        if not existing_time:
            continue

        existing_start = time_to_minutes(existing_time)
        existing_duration = int(
            get_service_info(business_id, row["service"]).get("duration", 45)
        )
        existing_end = existing_start + existing_duration
        existing_intervals.append((existing_start, existing_end))

    free_slots = []
    current = open_minutes

    while current + new_duration <= close_minutes:
        new_end = current + new_duration

        overlaps = any(
            current < existing_end and existing_start < new_end
            for existing_start, existing_end in existing_intervals
        )

        if not overlaps:
            free_slots.append(current)

        current += step_min

    later_slots = [m for m in free_slots if m >= requested_minutes]
    earlier_slots = [m for m in free_slots if m < requested_minutes]

    selected = later_slots[:max_suggestions]

    if len(selected) < max_suggestions:
        needed = max_suggestions - len(selected)
        selected += earlier_slots[-needed:]

    selected = sorted(selected)

    return [f"{m // 60:02d}:{m % 60:02d}" for m in selected]


def save_reservation(
    business_id,
    phone,
    name,
    service,
    date,
    time_,
    resource_id=None,
    resource_name_snapshot=None,
):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO reservations (
                business_id,
                customer_name,
                customer_phone,
                service,
                date,
                time,
                status,
                resource_id,
                resource_name_snapshot
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'CONFIRMED', %s, %s)
            RETURNING id
            """,
            (business_id, name, phone, service, date, time_, resource_id, resource_name_snapshot),
        )
        row = c.fetchone()
        new_id = row["id"] if isinstance(row, dict) else row[0]
        conn.commit()
        print(
            f"SAVED (CONFIRMED) -> id={new_id}, {name}, {service} on {date} at {time_}, "
            f"resource_id={resource_id}, resource_name={resource_name_snapshot}"
        )
        return new_id
    except Exception as e:
        conn.rollback()
        print("save_reservation error:", str(e))
        raise
    finally:
        conn.close()

def time_to_minutes(time_str):
    h, m = map(int, time_str.split(":"))
    return h * 60 + m


def ranges_overlap(start1, duration1, start2, duration2):
    end1 = start1 + duration1
    end2 = start2 + duration2
    return start1 < end2 and start2 < end1


def is_slot_taken(business_id, date_iso, new_time, new_service):
    new_service_info = get_service_info(business_id, new_service)
    new_duration = int(new_service_info.get("duration", 45))
    new_start = time_to_minutes(new_time)

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT service, time
        FROM reservations
        WHERE business_id = %s
          AND date = %s
          AND status = 'CONFIRMED'
        """,
        (business_id, date_iso),
    )
    rows = c.fetchall()
    conn.close()

    for row in rows:
        existing_service = row["service"]
        existing_time = row["time"]

        normalized_existing_time = normalize_time_str(existing_time)
        if not normalized_existing_time:
            continue

        existing_service_info = get_service_info(business_id, existing_service)
        existing_duration = int(existing_service_info.get("duration", 45))
        existing_start = time_to_minutes(normalized_existing_time)

        if ranges_overlap(new_start, new_duration, existing_start, existing_duration):
            return True

    return False


def send_reservation_confirmation(
    phone,
    name,
    service,
    date,
    time,
    business,
    calendar_added=False,
    lang="en",
    resource_name=None,
):
    service_info = get_service_info(business["id"], service)
    total_price = service_info["price"]

    base_message = tr_confirmation(
        lang=lang,
        name=name,
        service=service,
        date=date,
        time=time,
        total_price=total_price,
        calendar_added=calendar_added,
        resource_name=resource_name,
    )

    send_friendly_message(phone, business, lang, base_message, purpose="confirmation")

def send_reservation_cancellation(
    phone,
    name,
    service,
    date,
    time,
    business,
    resource_name=None,
    lang=None,
):
    if not lang:
        preferred = (business.get("preferred_language") or "auto").strip().lower()
        lang = preferred if preferred in ("en", "fr", "ar") else "en"

    message = tr_cancellation(
        lang=lang,
        name=name,
        service=service,
        date=date,
        time=time,
        resource_name=resource_name,
    )

    send_friendly_message(phone, business, lang, message, purpose="cancel")


def send_reservation_rescheduled(
    phone,
    name,
    service,
    old_date,
    old_time,
    new_date,
    new_time,
    business,
    old_resource_name=None,
    new_resource_name=None,
    lang=None,
):
    if not phone:
        return False

    if not lang:
        preferred = (business.get("preferred_language") or "auto").strip().lower()
        lang = preferred if preferred in ("en", "fr", "ar") else "en"

    old_resource_line = ""
    new_resource_line = ""
    if old_resource_name:
        if lang == "fr":
            old_resource_line = f"\nAncien prestataire : {old_resource_name}"
        elif lang == "ar":
            old_resource_line = f"\nالموارد السابقة: {old_resource_name}"
        else:
            old_resource_line = f"\nPrevious staff/resource: {old_resource_name}"

    if new_resource_name:
        if lang == "fr":
            new_resource_line = f"\nNouveau prestataire : {new_resource_name}"
        elif lang == "ar":
            new_resource_line = f"\nالمورد الجديد: {new_resource_name}"
        else:
            new_resource_line = f"\nNew staff/resource: {new_resource_name}"

    if lang == "fr":
        message = (
            f"✅ Votre réservation a été reprogrammée.\n"
            f"Nom : {name}\n"
            f"Service : {service}\n"
            f"Ancienne date : {old_date}\n"
            f"Ancienne heure : {old_time}{old_resource_line}\n\n"
            f"Nouvelle date : {new_date}\n"
            f"Nouvelle heure : {new_time}{new_resource_line}"
        )
    elif lang == "ar":
        message = (
            f"✅ تم تعديل موعد حجزك.\n"
            f"الاسم: {name}\n"
            f"الخدمة: {service}\n"
            f"التاريخ السابق: {old_date}\n"
            f"الوقت السابق: {old_time}{old_resource_line}\n\n"
            f"التاريخ الجديد: {new_date}\n"
            f"الوقت الجديد: {new_time}{new_resource_line}"
        )
    else:
        message = (
            f"✅ Your reservation has been rescheduled.\n"
            f"Name: {name}\n"
            f"Service: {service}\n"
            f"Previous date: {old_date}\n"
            f"Previous time: {old_time}{old_resource_line}\n\n"
            f"New date: {new_date}\n"
            f"New time: {new_time}{new_resource_line}"
        )

    return send_friendly_message(phone, business, lang, message, purpose="confirmation")


def apply_reschedule_update(
    business,
    reservation,
    new_date,
    normalized_time,
    chosen_resource,
):
    business_id = business["id"]
    reservation_id = reservation["id"]

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        UPDATE reservations
        SET date = %s,
            time = %s,
            resource_id = %s,
            resource_name_snapshot = %s,
            status = CASE WHEN status = 'DONE' THEN 'CONFIRMED' ELSE status END
        WHERE id = %s AND business_id = %s
        """,
        (
            new_date,
            normalized_time,
            chosen_resource["id"] if chosen_resource else None,
            chosen_resource["name"] if chosen_resource else None,
            reservation_id,
            business_id,
        ),
    )
    conn.commit()
    conn.close()

    old_event_id = reservation.get("google_event_id")
    if old_event_id:
        try:
            delete_event(old_event_id, calendar_id=(business.get("calendar_id") or "primary"))
        except Exception as e:
            print("reschedule delete_event warning:", e, flush=True)

    new_event = add_reservation_to_google_calendar(
        business_id,
        reservation["customer_name"],
        reservation["service"],
        new_date,
        normalized_time,
        resource_name=chosen_resource["name"] if chosen_resource else None,
        resource_id=chosen_resource["id"] if chosen_resource else None,
    )
    if new_event and new_event.get("id"):
        save_google_event_id(reservation_id, new_event.get("id"))

    return new_event


def add_reservation_to_google_calendar(
    business_id,
    name,
    service,
    date,
    time_,
    resource_name=None,
    resource_id=None,
    extra_minutes=0,
):
    try:
        service_info = get_service_info(business_id, service)
        duration_min = int(service_info.get("duration", 45)) + int(extra_minutes or 0)
        business = get_business_by_id(business_id) or {}
        calendar_id = (business.get("calendar_id") or "primary").strip() or "primary"

        summary = f"{service} - {name}"
        if resource_name:
            summary += f" with {resource_name}"

        description = (
            f"Customer: {name}\n"
            f"Service: {service}\n"
            f"Date: {date}\n"
            f"Time: {time_}"
        )

        if resource_name:
            description += f"\nResource: {resource_name}"
        color_id = get_resource_calendar_color_id(
            business_id,
            resource_id=resource_id,
            resource_name=resource_name,
        )

        print("GCAL target calendar_id:", calendar_id, flush=True)

        try:
            event = create_event(
                summary=summary,
                date_str=date,
                time_str=time_,
                description=description,
                calendar_id=calendar_id,
                duration_min=duration_min,
                color_id=color_id,
            )
        except TypeError:
            event = create_event(
                summary=summary,
                date_str=date,
                time_str=time_,
                description=description,
                calendar_id=calendar_id,
                duration_min=duration_min,
            )

        print("Google Calendar event created:", event.get("id"), flush=True)
        return event

    except Exception as e:
        print("add_reservation_to_google_calendar error:", str(e), flush=True)
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


def update_reservation_note_value(reservation_id, business_id, note):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        UPDATE reservations
        SET notes = %s
        WHERE id = %s AND business_id = %s
        """,
        (note, reservation_id, business_id),
    )
    conn.commit()
    conn.close()

def get_confirmed_reservations_for_date_fast(business_id, date_iso):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, service, time, resource_id
        FROM reservations
        WHERE business_id = %s
          AND date = %s
          AND status = 'CONFIRMED'
        """,
        (business_id, date_iso),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def get_confirmed_reservations_for_date_excluding_fast(business_id, date_iso, excluded_reservation_id=None):
    rows = get_confirmed_reservations_for_date_fast(business_id, date_iso)
    if excluded_reservation_id is None:
        return rows
    return [row for row in rows if row.get("id") != excluded_reservation_id]


def compute_dashboard_report_metrics(business, services, reservations_rows):
    service_meta = {}
    for s in services:
        service_meta[(s["name"] or "").strip().lower()] = {
            "price": float(s.get("price") or 0),
            "duration": int(s.get("duration_min") or 45),
        }

    def get_meta(service_name):
        cleaned = (service_name or "").strip().lower()
        if cleaned in service_meta:
            return service_meta[cleaned]
        for name, meta in service_meta.items():
            if cleaned in name or name in cleaned:
                return meta
        return {"price": 0.0, "duration": 45}

    def summarize_metrics(rows):
        total = len(rows)
        confirmed = sum(1 for r in rows if (r.get("status") or "").upper() == "CONFIRMED")
        canceled = sum(1 for r in rows if (r.get("status") or "").upper() == "CANCELED")
        done = sum(1 for r in rows if (r.get("status") or "").upper() == "DONE")

        total_booked_revenue = 0.0
        total_done_revenue = 0.0
        service_counts = {}
        resource_counts = {}

        for r in rows:
            status = (r.get("status") or "").upper()
            meta = get_meta(r.get("service"))
            base_price = get_effective_service_price_for_reservation_row(business, r)
            extra_price = float(r.get("extra_price") or 0)
            price = base_price + extra_price

            if status in ("CONFIRMED", "DONE"):
                total_booked_revenue += price
            if status == "DONE":
                total_done_revenue += price

            service_name = (r.get("service") or "").strip()
            if service_name:
                service_counts[service_name] = service_counts.get(service_name, 0) + 1

            resource_name = (r.get("resource_name_snapshot") or "").strip()
            if resource_name:
                resource_counts[resource_name] = resource_counts.get(resource_name, 0) + 1

        return {
            "total_reservations": total,
            "confirmed_reservations": confirmed,
            "canceled_reservations": canceled,
            "done_reservations": done,
            "total_booked_revenue": total_booked_revenue,
            "total_done_revenue": total_done_revenue,
            "top_service": max(service_counts.items(), key=lambda x: x[1])[0] if service_counts else "-",
            "top_resource": max(resource_counts.items(), key=lambda x: x[1])[0] if resource_counts else "-",
        }

    def build_trend(current, previous, good_when="up"):
        if current == previous:
            return {
                "percent": 0,
                "direction": "flat",
                "good": False,
                "bad": False,
                "label": "No change vs previous 7 days",
            }

        if previous <= 0:
            percent = 100 if current > 0 else 0
        else:
            percent = int(round(abs(((current - previous) / previous) * 100)))

        direction = "up" if current > previous else "down"
        good = (direction == "up" and good_when == "up") or (direction == "down" and good_when == "down")
        bad = (direction == "up" and good_when == "down") or (direction == "down" and good_when == "up")

        return {
            "percent": percent,
            "direction": direction,
            "good": good,
            "bad": bad,
            "label": f"vs previous 7 days",
        }

    overall = summarize_metrics(reservations_rows)

    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")
    today = datetime.now(tz).date()
    current_start = today - timedelta(days=6)
    previous_start = today - timedelta(days=13)
    previous_end = today - timedelta(days=7)

    current_rows = []
    previous_rows = []
    for row in reservations_rows:
        try:
            row_date = datetime.strptime(row.get("date"), "%Y-%m-%d").date()
        except Exception:
            continue
        if current_start <= row_date <= today:
            current_rows.append(row)
        elif previous_start <= row_date <= previous_end:
            previous_rows.append(row)

    current_metrics = summarize_metrics(current_rows)
    previous_metrics = summarize_metrics(previous_rows)

    whatsapp_connected = bool(business.get("access_token"))
    minutes_per_reservation = 3 if whatsapp_connected else 2
    estimated_time_saved_minutes = overall["total_reservations"] * minutes_per_reservation

    savings_label = (
        "Estimated admin time saved with EzRezerve"
        if whatsapp_connected
        else "Estimated time saved vs manual tracking"
    )

    return {
        **overall,
        "estimated_time_saved_minutes": estimated_time_saved_minutes,
        "estimated_time_saved_hours": estimated_time_saved_minutes // 60,
        "estimated_time_saved_remainder_minutes": estimated_time_saved_minutes % 60,
        "savings_label": savings_label,
        "weekly_reservations_revenue": current_metrics["total_booked_revenue"],
        "previous_weekly_reservations_revenue": previous_metrics["total_booked_revenue"],
        "trend_total_reservations": build_trend(current_metrics["total_reservations"], previous_metrics["total_reservations"], good_when="up"),
        "trend_confirmed_reservations": build_trend(current_metrics["confirmed_reservations"], previous_metrics["confirmed_reservations"], good_when="up"),
        "trend_canceled_reservations": build_trend(current_metrics["canceled_reservations"], previous_metrics["canceled_reservations"], good_when="down"),
        "trend_done_reservations": build_trend(current_metrics["done_reservations"], previous_metrics["done_reservations"], good_when="up"),
        "trend_total_booked_revenue": build_trend(current_metrics["total_booked_revenue"], previous_metrics["total_booked_revenue"], good_when="up"),
        "trend_total_done_revenue": build_trend(current_metrics["total_done_revenue"], previous_metrics["total_done_revenue"], good_when="up"),
    }


def build_service_duration_cache(business_id, reservations_rows, extra_service_names=None):
    names = set()
    for row in reservations_rows:
        if row.get("service"):
            names.add(row["service"])

    if extra_service_names:
        for s in extra_service_names:
            if s:
                names.add(s)

    cache = {}
    for name in names:
        cache[name] = int(get_service_info(business_id, name).get("duration", 45))
    return cache


def is_resource_slot_full_fast(
    business_id,
    resource,
    reservations_rows,
    service_duration_cache,
    new_time,
    new_service,
):
    resource_id = resource["id"]
    new_duration = int(service_duration_cache.get(new_service, 45))
    new_start = time_to_minutes(new_time)
    new_units = get_service_capacity_units(business_id, new_service)
    shared_pool = get_service_shared_pool_key(business_id, new_service)
    capacity = 2 if shared_pool else int(resource.get("capacity") or 1)
    overlapping_units = 0
    for row in reservations_rows:
        existing_service = row.get("service")
        existing_pool = get_service_shared_pool_key(business_id, existing_service)
        same_pool = existing_pool == shared_pool if shared_pool else row.get("resource_id") == resource_id
        if not same_pool:
            continue
        existing_time = normalize_time_str(row["time"])
        if not existing_time:
            continue
        existing_start = time_to_minutes(existing_time)
        existing_duration = int(service_duration_cache.get(existing_service, 45))
        if ranges_overlap(new_start, new_duration, existing_start, existing_duration):
            overlapping_units += get_service_capacity_units(business_id, existing_service)
    return (overlapping_units + new_units) > capacity


def get_manual_reservation_resource_choice_fast(
    business_id,
    service_name,
    resource_id_raw,
    date_iso,
    time_,
    eligible_resources,
    reservations_rows,
    service_duration_cache,
):
    resource_id_raw = (resource_id_raw or "").strip()

    if not resource_id_raw or resource_id_raw == "auto":
        for r in eligible_resources:
            rules = get_resource_day_rules(business_id, r["id"], date_iso)
            if rules.get("closed"):
                continue
            if not is_time_within_business_hours(time_, rules["open_time"], rules["close_time"]):
                continue
            if not is_resource_slot_full_fast(
                business_id,
                r,
                reservations_rows,
                service_duration_cache,
                time_,
                service_name,
            ):
                return r, None
        return None, "No available staff/resource at that time."

    try:
        resource_id = int(resource_id_raw)
    except Exception:
        return None, "Invalid resource selected."

    resource = None
    for r in eligible_resources:
        if r["id"] == resource_id:
            resource = r
            break

    if not resource:
        resource = get_resource_by_id(resource_id, business_id)

    if not resource:
        return None, "Selected resource was not found."

    if not resource.get("is_active"):
        return None, "Selected resource is inactive."

    if not is_resource_allowed_for_service(business_id, resource_id, service_name):
        return None, "Selected resource cannot perform that service."

    rules = get_resource_day_rules(business_id, resource_id, date_iso)
    if rules.get("closed"):
        return None, "Selected resource is unavailable on that date."

    if not is_time_within_business_hours(time_, rules["open_time"], rules["close_time"]):
        return None, "Selected resource is outside working hours at that time."

    if is_resource_slot_full_fast(
        business_id,
        resource,
        reservations_rows,
        service_duration_cache,
        time_,
        service_name,
    ):
        return None, "Selected resource is already booked at that time."

    return resource, None

def get_manual_reservation_resource_choice(business_id, service_name, resource_id_raw, date_iso, time_):
    """
    Returns:
      (resource_row_or_none, error_message_or_none)
    """
    resource_id_raw = (resource_id_raw or "").strip()

    # Auto-assign if no specific resource selected
    if not resource_id_raw or resource_id_raw == "auto":
        eligible_resources = get_active_resources_for_service(business_id, service_name)
        for r in eligible_resources:
            rules = get_resource_day_rules(business_id, r["id"], date_iso)
            if rules.get("closed"):
                continue
            if not is_time_within_business_hours(time_, rules["open_time"], rules["close_time"]):
                continue
            if not is_resource_slot_full(business_id, r["id"], date_iso, time_, service_name):
                return r, None
        return None, "No available staff/resource at that time."

    try:
        resource_id = int(resource_id_raw)
    except Exception:
        return None, "Invalid resource selected."

    resource = get_resource_by_id(resource_id, business_id)
    if not resource:
        return None, "Selected resource was not found."

    if not resource.get("is_active"):
        return None, "Selected resource is inactive."

    if not is_resource_allowed_for_service(business_id, resource_id, service_name):
        return None, "Selected resource cannot perform that service."

    rules = get_resource_day_rules(business_id, resource_id, date_iso)
    if rules.get("closed"):
        return None, "Selected resource is unavailable on that date."

    if not is_time_within_business_hours(time_, rules["open_time"], rules["close_time"]):
        return None, "Selected resource is outside working hours at that time."

    if is_resource_slot_full(business_id, resource_id, date_iso, time_, service_name):
        return None, "Selected resource is already booked at that time."

    return resource, None


def get_confirmed_resource_reservations_for_date(business_id, date_iso, resource_ids):
    if not resource_ids:
        return {}

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT resource_id, service, time
        FROM reservations
        WHERE business_id = %s
          AND date = %s
          AND status = 'CONFIRMED'
          AND resource_id = ANY(%s)
        """,
        (business_id, date_iso, resource_ids),
    )
    rows = c.fetchall()
    conn.close()

    grouped = {}
    for row in rows:
        grouped.setdefault(row["resource_id"], []).append(row)
    return grouped


def get_service_duration_cached(business_id, service_name, cache=None):
    key = (service_name or "").strip().lower()
    if cache is not None and key in cache:
        return cache[key]
    duration = int(get_service_info(business_id, service_name).get("duration", 45))
    if cache is not None:
        cache[key] = duration
    return duration


def is_resource_slot_full_from_prefetched(business_id, resource, new_time, new_service, reservations_by_resource, service_duration_cache=None):
    new_duration = int(get_service_duration_cached(business_id, new_service, service_duration_cache))
    new_start = time_to_minutes(new_time)
    new_units = get_service_capacity_units(business_id, new_service)
    shared_pool = get_service_shared_pool_key(business_id, new_service)
    capacity = 2 if shared_pool else int(resource.get("capacity") or 1)
    overlapping_units = 0
    row_sets = reservations_by_resource.values() if shared_pool else [reservations_by_resource.get(resource["id"], [])]
    for rows in row_sets:
        for row in rows:
            existing_service = row.get("service")
            if shared_pool and get_service_shared_pool_key(business_id, existing_service) != shared_pool:
                continue
            existing_time = normalize_time_str(row["time"])
            if not existing_time:
                continue
            existing_duration = int(get_service_duration_cached(business_id, existing_service, service_duration_cache))
            existing_start = time_to_minutes(existing_time)
            if ranges_overlap(new_start, new_duration, existing_start, existing_duration):
                overlapping_units += get_service_capacity_units(business_id, existing_service)
    return (overlapping_units + new_units) > capacity

def reservation_has_ended(business, reservation):
    try:
        end_dt = reservation_end_datetime(
            business,
            reservation["date"],
            reservation["time"],
            reservation["service"],
            reservation.get("extra_minutes") or 0,
        )
        now_dt = datetime.now(pytz.timezone(business.get("timezone") or "Asia/Beirut"))
        return end_dt <= now_dt
    except Exception:
        return False


def dashboard_redirect_with_toast(message=None, toast_type="info", tab="reservations"):
    url = f"/dashboard?tab={tab}"
    if message:
        url += f"&toast_type={quote_plus(str(toast_type))}&toast={quote_plus(str(message))}"
    return redirect(url)


def should_attempt_calendar_sync(business):
    if business.get("gcal_credentials"):
        return True
    if os.path.exists("token.json") and os.path.exists("credentials.json"):
        return True
    return False

def _handle_manual_add_reservation():
    if "business_id" not in session:
        return redirect("/login")

    started_at = time.perf_counter()
    calendar_seconds = None

    try:
        business_id = session["business_id"]

        customer_name = (request.form.get("customer_name") or "").strip()
        customer_phone = (request.form.get("customer_phone") or "").strip()
        service = (request.form.get("service") or "").strip()
        date_iso = (request.form.get("date") or "").strip()
        time_raw = (request.form.get("time") or "").strip()
        resource_id_raw = (request.form.get("resource_id") or "auto").strip()
        notes = (request.form.get("notes") or request.form.get("note") or "").strip()

        if not customer_name or not service or not date_iso or not time_raw:
            return dashboard_redirect_with_toast("Please fill in all required fields.", "error")

        business = get_business_by_id(business_id)
        if not business:
            return dashboard_redirect_with_toast("Business not found.", "error")

        valid_service, _ = validate_service_for_business(business_id, service)
        if not valid_service:
            return dashboard_redirect_with_toast("Selected service is not valid.", "error")

        normalized_time = normalize_time_str_with_hours(time_raw)
        if not normalized_time:
            return dashboard_redirect_with_toast("Please choose a valid time.", "error")

        if is_past_reservation_datetime(business, date_iso, normalized_time):
            return dashboard_redirect_with_toast("Past reservations are not allowed from the dashboard.", "error")

        day_rules = get_day_rules(business_id, date_iso)
        if day_rules.get("closed"):
            return dashboard_redirect_with_toast("This date is closed for reservations.", "error")

        if not is_time_within_business_hours(
            normalized_time,
            day_rules["open_time"],
            day_rules["close_time"],
        ):
            return dashboard_redirect_with_toast("That time is outside business hours.", "error")

        # FAST PRELOADS
        reservations_rows = get_confirmed_reservations_for_date_fast(business_id, date_iso)
        eligible_resources = get_active_resources_for_service(business_id, valid_service)
        service_duration_cache = build_service_duration_cache(
            business_id,
            reservations_rows,
            extra_service_names=[valid_service],
        )

        chosen_resource = None

        if eligible_resources:
            chosen_resource, error_message = get_manual_reservation_resource_choice_fast(
                business_id,
                valid_service,
                resource_id_raw,
                date_iso,
                normalized_time,
                eligible_resources,
                reservations_rows,
                service_duration_cache,
            )
            if error_message:
                print("manual_add_reservation:", error_message, flush=True)
                return dashboard_redirect_with_toast(error_message, "error")
        else:
            if is_slot_taken(business_id, date_iso, normalized_time, valid_service):
                print("manual_add_reservation: business-wide slot already taken", flush=True)
                return dashboard_redirect_with_toast("This time slot is already taken.", "error")

        calendar_warning = None

        reservation_id = save_reservation(
            business_id,
            customer_phone,
            customer_name,
            valid_service,
            date_iso,
            normalized_time,
            resource_id=chosen_resource["id"] if chosen_resource else None,
            resource_name_snapshot=chosen_resource["name"] if chosen_resource else None,
        )

        if notes:
            update_reservation_note_value(reservation_id, business_id, notes)

        if should_attempt_calendar_sync(business):
            calendar_started = time.perf_counter()
            gcal_event = add_reservation_to_google_calendar(
                business_id,
                customer_name,
                valid_service,
                date_iso,
                normalized_time,
                resource_name=chosen_resource["name"] if chosen_resource else None,
                resource_id=chosen_resource["id"] if chosen_resource else None,
            )
            calendar_seconds = time.perf_counter() - calendar_started

            if gcal_event:
                event_id = gcal_event.get("id")
                if event_id:
                    save_google_event_id(reservation_id, event_id)
            else:
                calendar_warning = "Reservation saved, but Google Calendar sync failed. Check the server logs."
        else:
            calendar_warning = "Reservation saved, but Google Calendar is not connected."

        if calendar_warning:
            return dashboard_redirect_with_toast(calendar_warning, "warning")

        return dashboard_redirect_with_toast("Reservation added successfully.", "success")

    except Exception as e:
        print("manual_add_reservation error:", str(e), flush=True)
        return dashboard_redirect_with_toast("Reservation could not be saved. Please check the server logs.", "error")

    finally:
        total_seconds = time.perf_counter() - started_at
        print(f"manual_add_reservation total_seconds: {total_seconds:.3f}", flush=True)
        if calendar_seconds is not None:
            print(f"manual_add_reservation calendar_seconds: {calendar_seconds:.3f}", flush=True)

@app.route("/reservations/manual-add", methods=["POST"])
def manual_add_reservation():
    return _handle_manual_add_reservation()

def is_resource_allowed_for_service(business_id, resource_id, service_name):
    service_row = get_service_row_for_business(business_id, service_name)
    if not service_row:
        return False

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT 1
        FROM resource_services
        WHERE business_id = %s
        LIMIT 1
        """,
        (business_id,),
    )
    has_any_assignments = bool(c.fetchone())

    if not has_any_assignments:
        conn.close()
        return True

    c.execute(
        """
        SELECT 1
        FROM resource_services
        WHERE business_id = %s
          AND resource_id = %s
          AND service_id = %s
        LIMIT 1
        """,
        (business_id, resource_id, service_row["id"]),
    )
    row = c.fetchone()
    conn.close()
    return bool(row)

def apply_tone_to_text(business, text):
    tone = (business.get("assistant_tone") or "friendly").strip().lower()
    msg = (text or "").strip()

    if tone == "professional":
        replacements = [
            ("Hi! Welcome 👋", "Hello."),
            ("Hi!", "Hello."),
            ("Hello! Welcome 🤍", "Hello."),
            ("Sure —", "Certainly —"),
            ("Of course —", "Certainly —"),
            ("Thanks,", "Thank you,"),
            ("Thanks so much,", "Thank you,"),
            ("Perfect —", "Understood —"),
            ("Perfect 🤍 —", "Understood —"),
            ("Thank you for booking with us 🤍", "Thank you for your reservation."),
            ("If this is a mistake, please contact us to reschedule.", "If needed, please contact us to reschedule."),
            ("🤍", ""),
            ("👋", ""),
        ]
    elif tone == "warm":
        replacements = [
            ("Hi! Welcome 👋", "Hello! Welcome 🤍"),
            ("Hi!", "Hello!"),
            ("Sure —", "Of course —"),
            ("Thanks,", "Thanks so much,"),
            ("Perfect —", "Perfect 🤍 —"),
        ]
    elif tone == "luxury":
        replacements = [
            ("Hi! Welcome 👋", "Welcome."),
            ("Hi!", "Welcome."),
            ("Hello! Welcome 🤍", "Welcome."),
            ("Sure —", "Certainly —"),
            ("Of course —", "Certainly —"),
            ("Thanks,", "Thank you,"),
            ("Thanks so much,", "Thank you,"),
            ("Perfect —", "Wonderful —"),
            ("Perfect 🤍 —", "Wonderful —"),
            ("Thank you for booking with us 🤍", "We look forward to welcoming you."),
            ("👋", ""),
        ]
    else:
        replacements = []

    for old, new in replacements:
        msg = msg.replace(old, new)

    return msg.strip()

def get_confirmed_reservations_for_phone(business, phone):
    mark_past_reservations_done(business)

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, google_event_id, customer_name, customer_phone, service, date, time,
               resource_id, resource_name_snapshot, status
        FROM reservations
        WHERE business_id = %s
          AND customer_phone = %s
          AND status = 'CONFIRMED'
        ORDER BY id DESC
        """,
        (business["id"], phone),
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
        SET status = 'CANCELED'
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

def get_default_business_language(business):
    preferred = (business.get("preferred_language") or "auto").strip().lower()
    if preferred in ("en", "ar", "fr"):
        return preferred
    return "en"


def get_effective_language(business, text, state=None):
    preferred = (business.get("preferred_language") or "auto").strip().lower()

    # If owner chose a fixed language in the dashboard, always use it
    if preferred in ("en", "ar", "fr"):
        return preferred

    # Otherwise keep the conversation language stable once detected
    if state and state.get("lang") in ("en", "ar", "fr"):
        return state["lang"]

    return detect_lang(text)

def is_greeting(text):
    t = (text or "").strip().lower()

    greeting_keywords = [
        "hi", "hello", "hey", "hola", "bonjour", "salut",
        "مرحبا", "اهلا", "أهلا", "سلام",
        "kifak", "kifik"
    ]

    # exact or contains
    if any(g in t for g in greeting_keywords):
        return True

    # stretched greetings like helloo / hiiii / heyyy
    if t.startswith("hel") or t.startswith("hii") or t.startswith("hey"):
        return True

    return False

def get_business_greeting(business, lang):
    custom = (business.get("custom_welcome_message") or "").strip()
    if custom:
        return custom
    return tr(lang, "greeting")

def is_booking_intent(text):
    t = (text or "").strip().lower()
    booking_keywords = [
        "book", "booking", "reserve", "reservation", "appointment",
        "bonjour je veux reserver", "réserver", "reserver", "rdv",
        "احجز", "أحجز", "حجز", "موعد", "بدي احجز", "اريد احجز",
        "bede ehjoz", "bade ehjoz", "ehjoz", "ehجز", "7جز", "7joz",
        "bede oss", "bade oss", "oss cha3re", "oss sha3re", "bede oss cha3re",
        "bade oss cha3re", "bede 2oss cha3re", "bade 2oss cha3re"
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

def is_reschedule_intent(text):
    t = (text or "").strip().lower()
    reschedule_keywords = [
        "reschedule", "change my reservation", "change appointment", "move my reservation",
        "move appointment", "reschedule appointment", "reschedule reservation",
        "modifier reservation", "changer reservation", "decaler reservation", "reporter reservation",
        "تغيير الحجز", "غير الحجز", "بدل الموعد", "بدي غير الحجز", "أجل الحجز", "اجل الحجز",
        "ghayer el hajz", "ghayer l hajz", "bade ghayer", "bede ghayer"
    ]
    return any(k in t for k in reschedule_keywords)

def is_yes_intent(text):
    t = (text or "").strip().lower()
    yes_keywords = {
        "yes", "yeah", "yep", "ok", "okay", "sure", "of course",
        "oui", "d'accord", "dakhel", "تمام", "اي", "نعم", "اوكي", "أكيد", "اكيد"
    }
    return t in yes_keywords or any(k == t for k in yes_keywords)


def is_no_intent(text):
    t = (text or "").strip().lower()
    no_keywords = {
        "no", "nope", "nah",
        "non",
        "لا", "لأ", "مش", "مش هيدا", "لا شكرا", "لا شكراً"
    }
    return t in no_keywords or any(k == t for k in no_keywords)


def tr_switch_offer(lang, preferred_name, offered_name, date_, time_, nearby_text=""):
    if lang == "fr":
        msg = (
            f"{preferred_name} n’est pas disponible le {date_} à {time_}.\n"
            f"{offered_name} est disponible à la même heure.\n"
            f"Voulez-vous réserver avec {offered_name} ?"
        )
        if nearby_text:
            msg += f"\n\nHeures proches avec {preferred_name} :\n{nearby_text}"
        return msg

    if lang == "ar":
        msg = (
            f"{preferred_name} غير متاح بتاريخ {date_} الساعة {time_}.\n"
            f"{offered_name} متاح بنفس الوقت.\n"
            f"بدك أحجز مع {offered_name}؟"
        )
        if nearby_text:
            msg += f"\n\nأقرب الأوقات مع {preferred_name}:\n{nearby_text}"
        return msg

    msg = (
        f"{preferred_name} isn’t available on {date_} at {time_}.\n"
        f"{offered_name} is available at the same time.\n"
        f"Would you like to book with {offered_name} instead?"
    )
    if nearby_text:
        msg += f"\n\nClosest times with {preferred_name}:\n{nearby_text}"
    return msg


def tr_switch_declined(lang, preferred_name, nearby_text=""):
    if lang == "fr":
        if nearby_text:
            return f"D’accord — voici les heures proches avec {preferred_name} :\n{nearby_text}"
        return f"D’accord — aucune heure proche n’a été trouvée avec {preferred_name}."

    if lang == "ar":
        if nearby_text:
            return f"أكيد — هيدي أقرب الأوقات مع {preferred_name}:\n{nearby_text}"
        return f"أكيد — ما لقينا أوقات قريبة مع {preferred_name}."

    if nearby_text:
        return f"Okay — here are the closest times with {preferred_name}:\n{nearby_text}"
    return f"Okay — no nearby times were found with {preferred_name}."

def tr(lang, key, **kwargs):
    messages = {
        "greeting": {
            "en": "Hi! Welcome 👋\n\nHow can I help you today?\n• Type *book* to make a reservation\n• Type *cancel* to cancel your reservation\n• Type *reschedule* to move your reservation",
            "ar": "أهلاً 👋\n\nكيف فيني ساعدك اليوم؟\n• اكتب *احجز* لتعمل حجز\n• اكتب *الغاء* لتلغي الحجز\n• اكتب *تغيير الحجز* لتغيير الموعد",
            "fr": "Bonjour 👋\n\nComment puis-je vous aider aujourd’hui ?\n• Tapez *book* pour réserver\n• Tapez *cancel* pour annuler votre réservation\n• Tapez *reschedule* pour déplacer votre réservation",
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
        "past_date": {
            "en": "That date is in the past. Please choose today or a future date.",
            "ar": "هذا التاريخ أصبح بالماضي. اختار اليوم أو تاريخ بالمستقبل.",
            "fr": "Cette date est déjà passée. Veuillez choisir aujourd’hui ou une date future.",
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
WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def tr_confirmation(lang, name, service, date, time, total_price, calendar_added=False, resource_name=None):
    resource_line_map = {
        "en": f"\nWith: {resource_name}" if resource_name else "",
        "fr": f"\nAvec : {resource_name}" if resource_name else "",
        "ar": f"\nمع: {resource_name}" if resource_name else "",
    }
    calendar_line_map = {
        "en": "\n🗓 Also added to our Google Calendar." if calendar_added else "",
        "fr": "\n🗓 Également ajouté à notre Google Calendar." if calendar_added else "",
        "ar": "\n🗓 وتمت إضافته أيضاً إلى Google Calendar." if calendar_added else "",
    }
    messages = {
        "en": f"✅ Your reservation is confirmed!\nName: {name}\nService: {service}{resource_line_map['en']}\nDate: {date}\nTime: {time}{calendar_line_map['en']}\n\nThank you for booking with us 🤍",
        "fr": f"✅ Votre réservation est confirmée !\nNom : {name}\nService : {service}{resource_line_map['fr']}\nDate : {date}\nHeure : {time}{calendar_line_map['fr']}\n\nMerci pour votre réservation 🤍",
        "ar": f"✅ تم تأكيد حجزك!\nالاسم: {name}\nالخدمة: {service}{resource_line_map['ar']}\nالتاريخ: {date}\nالوقت: {time}{calendar_line_map['ar']}\n\nشكراً لحجزك معنا 🤍",
    }
    return messages.get(lang, messages["en"])

def tr_cancellation(lang, name, service, date, time, resource_name=None):
    resource_line_map = {
        "en": f"\nWith: {resource_name}" if resource_name else "",
        "fr": f"\nAvec : {resource_name}" if resource_name else "",
        "ar": f"\nمع: {resource_name}" if resource_name else "",
    }

    messages = {
        "en": (
            f"❌ Your reservation has been canceled.\n"
            f"Name: {name}\n"
            f"Service: {service}{resource_line_map['en']}\n"
            f"Date: {date}\n"
            f"Time: {time}\n\n"
            f"If this is a mistake, please contact us to reschedule."
        ),
        "fr": (
            f"❌ Votre réservation a été annulée.\n"
            f"Nom : {name}\n"
            f"Service : {service}{resource_line_map['fr']}\n"
            f"Date : {date}\n"
            f"Heure : {time}\n\n"
            f"Si c’est une erreur, veuillez nous contacter pour reprogrammer."
        ),
        "ar": (
            f"❌ تم إلغاء حجزك.\n"
            f"الاسم: {name}\n"
            f"الخدمة: {service}{resource_line_map['ar']}\n"
            f"التاريخ: {date}\n"
            f"الوقت: {time}\n\n"
            f"إذا كان هذا عن طريق الخطأ، يرجى التواصل معنا لإعادة الحجز."
        ),
    }

    return messages.get(lang, messages["en"])


def tr_resource_unavailable(lang, resource_name, date, time_, same_time_names="", nearby_text="", no_alternatives=False):
    if lang == "fr":
        parts = [
            f"Désolé, {resource_name} n’est pas disponible le {date} à {time_}."
        ]
        if same_time_names:
            parts.append(f"Disponible à la même heure : {same_time_names}.")
        if nearby_text:
            parts.append(f"Heures proches avec {resource_name} :\n{nearby_text}")
        if no_alternatives:
            parts.append("Aucune alternative proche n’a été trouvée.")
        return "\n".join(parts)

    if lang == "ar":
        parts = [
            f"عذراً، {resource_name} غير متاح بتاريخ {date} الساعة {time_}."
        ]
        if same_time_names:
            parts.append(f"المتاحون في نفس الوقت: {same_time_names}.")
        if nearby_text:
            parts.append(f"أقرب الأوقات مع {resource_name}:\n{nearby_text}")
        if no_alternatives:
            parts.append("لم يتم العثور على بدائل قريبة.")
        return "\n".join(parts)

    parts = [
        f"Sorry, {resource_name} isn’t available on {date} at {time_}."
    ]
    if same_time_names:
        parts.append(f"Available at the same time: {same_time_names}.")
    if nearby_text:
        parts.append(f"Closest times with {resource_name}:\n{nearby_text}")
    if no_alternatives:
        parts.append("No nearby alternatives were found.")
    return "\n".join(parts)

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

def is_past_date_only(business, date_iso):
    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")
    today_iso = datetime.now(tz).date().isoformat()
    return date_iso < today_iso


def is_past_reservation_datetime(business, date_iso, time_str):
    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")
    normalized_time = normalize_time_str(time_str) or time_str
    target_naive = datetime.strptime(f"{date_iso} {normalized_time}", "%Y-%m-%d %H:%M")
    target_dt = tz.localize(target_naive)
    now_dt = datetime.now(tz)
    return target_dt < now_dt


def reservation_end_datetime(business, date_str, time_str, service_name, extra_minutes=0):
    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")

    normalized_time = normalize_time_str(time_str)
    if not normalized_time:
        normalized_time = time_str

    start_naive = datetime.strptime(f"{date_str} {normalized_time}", "%Y-%m-%d %H:%M")
    start_dt = tz.localize(start_naive)

    duration_min = get_reservation_total_duration_minutes(
        business["id"],
        service_name,
        extra_minutes=extra_minutes,
    )

    return start_dt + timedelta(minutes=duration_min)


def mark_past_reservations_done(business):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT id, date, time, service, COALESCE(extra_minutes, 0) AS extra_minutes
        FROM reservations
        WHERE business_id = %s
          AND status = 'CONFIRMED'
        """,
        (business["id"],),
    )
    rows = c.fetchall()

    now = datetime.now(pytz.timezone(business.get("timezone") or "Asia/Beirut"))
    ids_to_mark_done = []

    for row in rows:
        try:
            end_dt = reservation_end_datetime(
                business,
                row["date"],
                row["time"],
                row["service"],
                row.get("extra_minutes") or 0,
            )
            if end_dt <= now:
                ids_to_mark_done.append(row["id"])
        except Exception as e:
            print("mark_past_reservations_done error on row:", row, e)

    if ids_to_mark_done:
        for reservation_id in ids_to_mark_done:
            c.execute(
                """
                UPDATE reservations
                SET status = 'DONE'
                WHERE id = %s
                """,
                (reservation_id,),
            )
        conn.commit()

    conn.close()

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

    # Normal same-day range, e.g. 09:00 -> 18:00
    if start <= end:
        return start <= chosen <= end

    # Overnight range, e.g. 16:00 -> 00:00 or 16:00 -> 02:00
    return chosen >= start or chosen <= end

def humanize_reply(lang, fallback_text, purpose="general"):
    return fallback_text

def send_friendly_message(phone, business, lang, text, purpose="general"):
    toned_text = apply_tone_to_text(business, text)
    final_text = humanize_reply(lang, toned_text, purpose=purpose)
    send_message(phone, final_text, business)

import re

def normalize_time_str_with_hours(time_input, open_time=None, close_time=None):
    raw = (time_input or "").strip().lower()

    # Arabic digits to English
    arabic_digits_map = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    raw = raw.translate(arabic_digits_map)

    # explicit am/pm
    m = re.match(r"^\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*$", raw)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2) or 0)
        suffix = m.group(3)

        if suffix == "pm" and hour != 12:
            hour += 12
        if suffix == "am" and hour == 12:
            hour = 0

        return f"{hour:02d}:{minute:02d}"

    # plain numeric time like 4:30 or 16:30
    m = re.match(r"^\s*(\d{1,2})(?::(\d{2}))?\s*$", raw)
    if not m:
        return normalize_time_str(time_input)

    hour = int(m.group(1))
    minute = int(m.group(2) or 0)

    # already 24-hour style
    if hour > 12:
        return f"{hour:02d}:{minute:02d}"

    # ambiguous time: try both AM and PM versions
    candidates = []

    # AM version
    am_hour = 0 if hour == 12 else hour
    candidates.append(f"{am_hour:02d}:{minute:02d}")

    # PM version
    if hour == 12:
        pm_hour = 12
    else:
        pm_hour = hour + 12
    candidates.append(f"{pm_hour:02d}:{minute:02d}")

    # if business hours are known, prefer the one inside them
    if open_time and close_time:
        valid = [c for c in candidates if is_time_within_business_hours(c, open_time, close_time)]
        if len(valid) == 1:
            return valid[0]
        if len(valid) > 1:
            return valid[0]

    # fallback: prefer PM for daytime business use
    return candidates[1]

def validate_service_for_business(business_id, service_name):
    services = get_service_names_for_business(business_id)
    cleaned = (service_name or "").strip().lower()

    if not services:
        return None, services

    # Exact match first
    services_map = {s.strip().lower(): s for s in services}
    if cleaned in services_map:
        return services_map[cleaned], services

    # Partial match
    for s in services:
        s_norm = s.strip().lower()
        if cleaned in s_norm or s_norm in cleaned:
            return s, services

    # Alias-based fallback
    aliases = {
        "beard": "beard",
        "beard trim": "beard",
        "shave": "beard",
        "haircut": "hair",
        "cut": "hair",
        "hair": "hair",
        "pedicure": "pedicure",
        "manicure": "manicure",
    }

    wanted = aliases.get(cleaned)
    if wanted:
        for s in services:
            s_norm = s.strip().lower()
            if wanted in s_norm:
                return s, services

    return None, services
def get_service_row_for_business(business_id, service_name):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT id, name, price, duration_min, sport_category, night_price, capacity_units_used
        FROM services
        WHERE business_id = %s
          AND lower(trim(name)) = lower(trim(%s))
        LIMIT 1
        """,
        (business_id, service_name),
    )
    row = c.fetchone()

    if not row:
        c.execute(
            """
            SELECT id, name, price, duration_min
            FROM services
            WHERE business_id = %s
              AND lower(name) LIKE lower(%s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (business_id, f"%{service_name.strip()}%"),
        )
        row = c.fetchone()

    conn.close()
    return row


def get_active_resources_for_service(business_id, service_name):
    """
    If no resource-service assignments exist for the business yet,
    treat all active resources as eligible.
    Once assignments exist, only assigned resources are eligible.
    """
    service_row = get_service_row_for_business(business_id, service_name)

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT 1
        FROM resource_services
        WHERE business_id = %s
        LIMIT 1
        """,
        (business_id,),
    )
    has_any_assignments = bool(c.fetchone())

    if not has_any_assignments:
        c.execute(
            """
            SELECT *
            FROM resources
            WHERE business_id = %s
              AND is_active = TRUE
            ORDER BY display_order ASC, id ASC
            """,
            (business_id,),
        )
        rows = c.fetchall()
        conn.close()
        return rows

    if not service_row:
        conn.close()
        return []

    c.execute(
        """
        SELECT r.*
        FROM resources r
        JOIN resource_services rs ON rs.resource_id = r.id
        WHERE r.business_id = %s
          AND r.is_active = TRUE
          AND rs.service_id = %s
        ORDER BY r.display_order ASC, r.id ASC
        """,
        (business_id, service_row["id"]),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def extract_requested_resource_from_text(text, resources):
    """
    If the customer writes something like:
      - '4 with Charbel'
      - 'with Jules at 5'
    detect the resource name from the text.
    """
    lt = (text or "").strip().lower()
    best = None
    best_len = -1

    for r in resources:
        name = (r.get("name") or "").strip().lower()
        if name and name in lt:
            if len(name) > best_len:
                best = r
                best_len = len(name)

    return best


def get_resource_day_rules(business_id, resource_id, date_iso):
    target_date = datetime.strptime(date_iso, "%Y-%m-%d").date()
    weekday = target_date.weekday()

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT id
        FROM resource_blocked_dates
        WHERE business_id = %s
          AND resource_id = %s
          AND blocked_date = %s
        """,
        (business_id, resource_id, date_iso),
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
        FROM resource_hours
        WHERE business_id = %s
          AND resource_id = %s
          AND weekday = %s
        """,
        (business_id, resource_id, weekday),
    )
    row = c.fetchone()
    conn.close()

    if not row:
        return get_day_rules(business_id, date_iso)

    if row["is_closed"]:
        return {"blocked": False, "closed": True, "reason": "weekly_closed"}

    return {
        "blocked": False,
        "closed": False,
        "open_time": row["open_time"],
        "close_time": row["close_time"],
    }


def is_resource_slot_full(business_id, resource_id, date_iso, new_time, new_service):
    resource = get_resource_by_id(resource_id, business_id)
    if not resource:
        return True
    new_duration = int(get_service_info(business_id, new_service).get("duration", 45))
    new_start = time_to_minutes(new_time)
    new_units = get_service_capacity_units(business_id, new_service)
    shared_pool = get_service_shared_pool_key(business_id, new_service)
    capacity = 2 if shared_pool else int(resource.get("capacity") or 1)
    conn = get_db_connection(); c = conn.cursor()
    c.execute("""SELECT service, time, resource_id, COALESCE(extra_minutes, 0) AS extra_minutes
                 FROM reservations
                 WHERE business_id = %s AND date = %s AND status = 'CONFIRMED'""", (business_id, date_iso))
    rows = c.fetchall(); conn.close()
    overlapping_units = 0
    for row in rows:
        existing_service = row["service"]
        existing_pool = get_service_shared_pool_key(business_id, existing_service)
        same_pool = existing_pool == shared_pool if shared_pool else row.get("resource_id") == resource_id
        if not same_pool:
            continue
        existing_time = normalize_time_str(row["time"])
        if not existing_time:
            continue
        existing_duration = get_reservation_total_duration_minutes(business_id, existing_service, extra_minutes=row.get("extra_minutes") or 0)
        existing_start = time_to_minutes(existing_time)
        if ranges_overlap(new_start, new_duration, existing_start, existing_duration):
            overlapping_units += get_service_capacity_units(business_id, existing_service)
    return (overlapping_units + new_units) > capacity


def get_available_resources_for_slot(business_id, date_iso, time_, service_name):
    eligible_resources = get_active_resources_for_service(business_id, service_name)
    available = []

    for r in eligible_resources:
        rules = get_resource_day_rules(business_id, r["id"], date_iso)
        if rules.get("closed"):
            continue

        if not is_time_within_business_hours(time_, rules["open_time"], rules["close_time"]):
            continue

        if not is_resource_slot_full(business_id, r["id"], date_iso, time_, service_name):
            available.append(r)

    return available


def suggest_slots_for_resource(
    business_id,
    resource_id,
    date_iso,
    requested_time_str,
    service_name,
    max_suggestions=3,
):
    rules = get_resource_day_rules(business_id, resource_id, date_iso)
    if rules.get("closed"):
        return []

    open_start = rules["open_time"]
    open_end = rules["close_time"]

    requested_norm = normalize_time_str_with_hours(
        requested_time_str,
        open_start,
        open_end,
    ) or open_start

    requested_minutes = time_to_minutes(requested_norm)
    open_minutes = time_to_minutes(open_start)
    close_minutes = time_to_minutes(open_end)

    duration = int(get_service_info(business_id, service_name).get("duration", 45))

    free_slots = []
    current = open_minutes

    while current + duration <= close_minutes:
        hhmm = f"{current // 60:02d}:{current % 60:02d}"
        if not is_resource_slot_full(business_id, resource_id, date_iso, hhmm, service_name):
            free_slots.append(current)
        current += 15

    later_slots = [m for m in free_slots if m >= requested_minutes]
    earlier_slots = [m for m in free_slots if m < requested_minutes]

    selected = later_slots[:max_suggestions]
    if len(selected) < max_suggestions:
        needed = max_suggestions - len(selected)
        selected += earlier_slots[-needed:]

    selected = sorted(selected)
    return [f"{m // 60:02d}:{m % 60:02d}" for m in selected]


def suggest_resource_options(
    business_id,
    date_iso,
    requested_time_str,
    service_name,
    max_suggestions=3,
):
    """
    Return combined alternatives like:
      16:00 with Jules
      16:15 with Charbel
    """
    eligible_resources = get_active_resources_for_service(business_id, service_name)
    if not eligible_resources:
        return []

    requested_norm = normalize_time_str(requested_time_str) or requested_time_str
    requested_minutes = time_to_minutes(requested_norm)

    duration = int(get_service_info(business_id, service_name).get("duration", 45))
    candidates = []

    for r in eligible_resources:
        rules = get_resource_day_rules(business_id, r["id"], date_iso)
        if rules.get("closed"):
            continue

        open_minutes = time_to_minutes(rules["open_time"])
        close_minutes = time_to_minutes(rules["close_time"])

        current = open_minutes
        while current + duration <= close_minutes:
            hhmm = f"{current // 60:02d}:{current % 60:02d}"
            if not is_resource_slot_full(business_id, r["id"], date_iso, hhmm, service_name):
                candidates.append({
                    "time": hhmm,
                    "resource_name": r["name"],
                    "distance": abs(current - requested_minutes),
                })
            current += 15

    candidates.sort(key=lambda x: (x["distance"], x["time"], x["resource_name"]))

    seen = set()
    final = []
    for item in candidates:
        key = (item["time"], item["resource_name"])
        if key in seen:
            continue
        seen.add(key)
        final.append(item)
        if len(final) >= max_suggestions:
            break

    return final

def process_incoming_message(business, phone, text):
    global user_state

    t = (text or "").strip()
    lt = t.lower()

    key = (business["id"], phone)
    state = user_state.get(key)

    lang = get_effective_language(business, t, state)

    # GREETING
    if is_greeting(t):
        send_friendly_message(
            phone,
            business,
            lang,
            get_business_greeting(business, lang),
            purpose="greeting",
        )
        return "ok", 200

    # START BOOKING
    if is_booking_intent(lt):
        user_state[key] = {"step": "awaiting_name", "lang": lang}
        send_friendly_message(phone, business, lang, tr(lang, "ask_name"), purpose="ask_name")
        return "ok", 200

    # CANCEL BOOKING
    if is_cancel_intent(lt):
        reservations = get_confirmed_reservations_for_phone(business, phone)

        if not reservations:
            send_friendly_message(phone, business, lang, tr(lang, "no_active_cancel"), purpose="cancel")
            return "ok", 200

        deleted_count = 0
        for r in reservations:
            event_id = r.get("google_event_id")
            if event_id:
                if delete_event(event_id, calendar_id=(business.get("calendar_id") or "primary")):
                    deleted_count += 1

        cancelled_count = mark_reservations_cancelled_by_phone(business["id"], phone)

        send_friendly_message(
            phone,
            business,
            lang,
            tr(lang, "cancel_done", count=cancelled_count, events=deleted_count),
            purpose="cancel",
        )
        return "ok", 200

    # RESCHEDULE BOOKING
    if is_reschedule_intent(lt):
        reservations = get_confirmed_reservations_for_phone(business, phone)

        if not reservations:
            no_reschedule_map = {
                "en": "You have no active reservation to reschedule.",
                "fr": "Vous n’avez aucune réservation active à reprogrammer.",
                "ar": "ما عندك حجز مفعّل لتغيير موعده.",
            }
            send_friendly_message(
                phone,
                business,
                lang,
                no_reschedule_map.get(lang, no_reschedule_map["en"]),
                purpose="reschedule",
            )
            return "ok", 200

        reservation = reservations[0]
        user_state[key] = {
            "step": "awaiting_reschedule_date",
            "lang": lang,
            "reschedule_reservation_id": reservation["id"],
            "name": reservation.get("customer_name", ""),
            "service": reservation.get("service", ""),
            "old_date": reservation.get("date", ""),
            "old_time": reservation.get("time", ""),
            "resource_id": reservation.get("resource_id"),
            "resource_name": reservation.get("resource_name_snapshot"),
        }

        ask_reschedule_date_map = {
            "en": f"I found your active reservation for {reservation.get('service', '')} on {reservation.get('date', '')} at {reservation.get('time', '')}. What new date would you like?",
            "fr": f"J’ai trouvé votre réservation active pour {reservation.get('service', '')} le {reservation.get('date', '')} à {reservation.get('time', '')}. Quelle nouvelle date souhaitez-vous ?",
            "ar": f"لقيت حجزك المفعّل لخدمة {reservation.get('service', '')} بتاريخ {reservation.get('date', '')} الساعة {reservation.get('time', '')}. أي تاريخ جديد بدك؟",
        }
        send_friendly_message(
            phone,
            business,
            lang,
            ask_reschedule_date_map.get(lang, ask_reschedule_date_map["en"]),
            purpose="ask_date",
        )
        return "ok", 200

    # STEP RESCHEDULE – DATE
    if state and state.get("step") == "awaiting_reschedule_date":
        lang = state.get("lang", lang)

        if is_booking_intent(t) or is_cancel_intent(t) or is_reschedule_intent(t):
            repeat_date_map = {
                "en": "Please send the new date you want for your reservation.",
                "fr": "Veuillez envoyer la nouvelle date souhaitée pour votre réservation.",
                "ar": "من فضلك ابعت التاريخ الجديد اللي بدك ياه للحجز.",
            }
            send_friendly_message(phone, business, lang, repeat_date_map.get(lang, repeat_date_map["en"]), purpose="ask_date")
            return "ok", 200

        try:
            normalized_date = normalize_booking_date(t)
        except Exception:
            send_friendly_message(phone, business, lang, tr(lang, "invalid_date"), purpose="ask_date")
            return "ok", 200

        if is_past_date_only(business, normalized_date):
            send_friendly_message(phone, business, lang, tr(lang, "past_date"), purpose="ask_date")
            return "ok", 200

        day_rules = get_day_rules(business["id"], normalized_date)
        if day_rules.get("closed"):
            send_friendly_message(phone, business, lang, tr(lang, "closed_day"), purpose="availability")
            return "ok", 200

        state["new_date"] = normalized_date
        state["step"] = "awaiting_reschedule_time"

        ask_reschedule_time_map = {
            "en": f"Great — what new time would you like on {normalized_date}?",
            "fr": f"Parfait — quelle nouvelle heure souhaitez-vous le {normalized_date} ?",
            "ar": f"ممتاز — أي وقت جديد بدك بتاريخ {normalized_date}؟",
        }
        send_friendly_message(phone, business, lang, ask_reschedule_time_map.get(lang, ask_reschedule_time_map["en"]), purpose="ask_time")
        return "ok", 200

    # STEP RESCHEDULE – TIME
    if state and state.get("step") == "awaiting_reschedule_time":
        lang = state.get("lang", lang)

        if is_booking_intent(t) or is_cancel_intent(t) or is_reschedule_intent(t):
            repeat_time_map = {
                "en": "Please send the new time you want for your reservation.",
                "fr": "Veuillez envoyer la nouvelle heure souhaitée pour votre réservation.",
                "ar": "من فضلك ابعت الوقت الجديد اللي بدك ياه للحجز.",
            }
            send_friendly_message(phone, business, lang, repeat_time_map.get(lang, repeat_time_map["en"]), purpose="ask_time")
            return "ok", 200

        new_date = state.get("new_date")
        day_rules = get_day_rules(business["id"], new_date)
        if day_rules.get("closed"):
            send_friendly_message(phone, business, lang, tr(lang, "closed_day"), purpose="availability")
            return "ok", 200

        normalized_time = normalize_time_str_with_hours(
            t,
            day_rules.get("open_time"),
            day_rules.get("close_time"),
        )
        if not normalized_time:
            send_friendly_message(phone, business, lang, tr(lang, "invalid_time"), purpose="ask_time")
            return "ok", 200

        if not is_time_within_business_hours(normalized_time, day_rules["open_time"], day_rules["close_time"]):
            send_friendly_message(phone, business, lang, tr(lang, "outside_hours"), purpose="availability")
            return "ok", 200

        reservation_id = state.get("reschedule_reservation_id")
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            """
            SELECT id, customer_name, customer_phone, service, status, google_event_id,
                   resource_id, resource_name_snapshot, date, time
            FROM reservations
            WHERE id = %s AND business_id = %s
            LIMIT 1
            """,
            (reservation_id, business["id"]),
        )
        reservation = c.fetchone()
        conn.close()

        if not reservation:
            user_state.pop(key, None)
            missing_map = {
                "en": "I could not find your reservation anymore. Please send book to create a new one.",
                "fr": "Je n’ai plus trouvé votre réservation. Veuillez envoyer book pour créer une nouvelle réservation.",
                "ar": "ما عاد لقيت الحجز. ابعت احجز إذا بدك تعمل حجز جديد.",
            }
            send_friendly_message(phone, business, lang, missing_map.get(lang, missing_map["en"]), purpose="error")
            return "ok", 200

        reservations_rows = get_confirmed_reservations_for_date_excluding_fast(
            business["id"],
            new_date,
            excluded_reservation_id=reservation_id,
        )
        valid_service, _ = validate_service_for_business(business["id"], reservation["service"])
        if not valid_service:
            send_friendly_message(phone, business, lang, tr(lang, "save_error"), purpose="error")
            user_state.pop(key, None)
            return "ok", 200

        eligible_resources = get_active_resources_for_service(business["id"], valid_service)
        service_duration_cache = build_service_duration_cache(
            business["id"],
            reservations_rows,
            extra_service_names=[valid_service],
        )

        selected_resource_raw = str(state.get("resource_id")) if state.get("resource_id") else "auto"
        chosen_resource = None

        if eligible_resources:
            chosen_resource, error_message = get_manual_reservation_resource_choice_fast(
                business["id"],
                valid_service,
                selected_resource_raw,
                new_date,
                normalized_time,
                eligible_resources,
                reservations_rows,
                service_duration_cache,
            )
            if error_message:
                send_friendly_message(phone, business, lang, error_message, purpose="slot_taken")
                return "ok", 200
        else:
            new_duration = int(service_duration_cache.get(valid_service, 45))
            new_start = time_to_minutes(normalized_time)
            for row in reservations_rows:
                existing_time = normalize_time_str(row["time"])
                if not existing_time:
                    continue
                existing_start = time_to_minutes(existing_time)
                existing_duration = int(service_duration_cache.get(row["service"], 45))
                if ranges_overlap(new_start, new_duration, existing_start, existing_duration):
                    send_friendly_message(phone, business, lang, tr(lang, "slot_taken", date=new_date, time=normalized_time), purpose="slot_taken")
                    return "ok", 200

        try:
            new_event = apply_reschedule_update(
                business,
                reservation,
                new_date,
                normalized_time,
                chosen_resource,
            )

            send_reservation_rescheduled(
                phone,
                reservation.get("customer_name", ""),
                valid_service,
                reservation.get("date", ""),
                reservation.get("time", ""),
                new_date,
                normalized_time,
                business,
                old_resource_name=reservation.get("resource_name_snapshot"),
                new_resource_name=chosen_resource["name"] if chosen_resource else None,
                lang=lang,
            )

            user_state.pop(key, None)
            return "ok", 200
        except Exception as e:
            print("customer reschedule error:", str(e), flush=True)
            send_friendly_message(phone, business, lang, tr(lang, "save_error"), purpose="error")
            return "ok", 200

    # STEP X – ALTERNATIVE RESOURCE CONFIRMATION
    if state and state.get("step") == "awaiting_alternative_confirmation":
        lang = state.get("lang", lang)
        offer = state.get("alternative_offer") or {}

        offered_resource_name = offer.get("offered_resource_name", "")
        preferred_resource_name = offer.get("preferred_resource_name", "")
        nearby_slots = offer.get("nearby_slots") or []

        user_text_lower = t.lower().strip()

        accepted = (
            is_yes_intent(t)
            or (offered_resource_name and offered_resource_name.lower() in user_text_lower)
        )

        if accepted:
            try:
                reservation_id = save_reservation(
                    business["id"],
                    phone,
                    state.get("name", ""),
                    state.get("service", ""),
                    state.get("date", ""),
                    state.get("time", ""),
                    resource_id=offer.get("offered_resource_id"),
                    resource_name_snapshot=offered_resource_name,
                )
                print("Reservation saved with id:", reservation_id)

                gcal_event = add_reservation_to_google_calendar(
                    business["id"],
                    state.get("name", ""),
                    state.get("service", ""),
                    state.get("date", ""),
                    state.get("time", ""),
                    resource_name=offered_resource_name,
                    resource_id=offer.get("offered_resource_id"),
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
                    lang=lang,
                    resource_name=offered_resource_name,
                )

                user_state.pop(key, None)
                return "ok", 200

            except Exception as e:
                print("STEP alternative confirmation save error:", str(e))
                send_friendly_message(phone, business, lang, tr(lang, "save_error"), purpose="error")
                return "ok", 200

        if is_no_intent(t):
            nearby_text = "\n".join([f"• {slot}" for slot in nearby_slots]) if nearby_slots else ""
            send_friendly_message(
                phone,
                business,
                lang,
                tr_switch_declined(lang, preferred_resource_name, nearby_text),
                purpose="slot_taken",
            )

            state["step"] = "awaiting_time"
            state.pop("alternative_offer", None)
            return "ok", 200

        # unclear answer → ask again
        repeat_map = {
            "en": f"Please reply yes to book with {offered_resource_name}, or no to see other times.",
            "fr": f"Veuillez répondre oui pour réserver avec {offered_resource_name}, ou non pour voir d’autres heures.",
            "ar": f"من فضلك جاوب نعم لنحجز مع {offered_resource_name}، أو لا لنشوف أوقات تانية.",
        }

        send_friendly_message(
            phone,
            business,
            lang,
            repeat_map.get(lang, repeat_map["en"]),
            purpose="slot_taken",
        )
        return "ok", 200

    # STEP 1 – NAME
    if state and state.get("step") == "awaiting_name":
        lang = state.get("lang", lang)

        # User repeated a command instead of giving a name
        if is_booking_intent(t) or is_cancel_intent(t):
            send_friendly_message(phone, business, lang, tr(lang, "ask_name"), purpose="ask_name")
            return "ok", 200

        state["name"] = t

        if should_use_sport_first_flow(business["id"]):
            sports = get_available_sports_for_business(business["id"])
            state["step"] = "awaiting_sport"
            sports_text = "\n".join([f"• {s.capitalize()}" for s in sports]) if sports else "• Padel\n• Basketball\n• Tennis"
            prompt_map = {
                "en": f"Thanks, {t}. Which sport would you like?\nAvailable sports:\n{sports_text}",
                "fr": f"Merci, {t}. Quel sport souhaitez-vous ?\nSports disponibles :\n{sports_text}",
                "ar": f"شكراً {t}. أي رياضة بدك؟\nالرياضات المتوفرة:\n{sports_text}",
            }
            send_friendly_message(phone, business, lang, prompt_map.get(lang, prompt_map["en"]), purpose="ask_sport")
            return "ok", 200

        state["step"] = "awaiting_service"
        services_text = format_service_bullets_for_business(business["id"])
        service_prompt_map = {
            "en": f"Thanks, {t}. Which service would you like?\nAvailable services:\n{services_text}",
            "fr": f"Merci, {t}. Quel service souhaitez-vous ?\nServices disponibles :\n{services_text}",
            "ar": f"شكراً {t}. أي خدمة بدك؟\nالخدمات المتوفرة:\n{services_text}",
        }
        send_friendly_message(phone, business, lang, service_prompt_map.get(lang, service_prompt_map["en"]), purpose="ask_service")
        return "ok", 200

    if state and state.get("step") == "awaiting_sport":
        lang = state.get("lang", lang)
        text_lower = t.lower().strip()
        available_sports = get_available_sports_for_business(business["id"])
        direct_service, direct_sport, _available = resolve_valid_service_and_sport(business["id"], t, None)
        if direct_service and direct_sport:
            state["selected_sport"] = direct_sport
            state["service"] = direct_service
            state["step"] = "awaiting_date"
            send_friendly_message(phone, business, lang, tr(lang, "ask_date", service=direct_service), purpose="ask_date")
            return "ok", 200
        chosen_sport = None
        for sport in available_sports:
            if sport.lower() == text_lower or sport.lower() in text_lower:
                chosen_sport = sport
                break
        if not chosen_sport:
            sports_text = "\n".join([f"• {s.capitalize()}" for s in available_sports]) if available_sports else "• Padel\n• Basketball\n• Tennis"
            retry_map = {
                "en": f"Please choose a sport first.\nAvailable sports:\n{sports_text}",
                "fr": f"Veuillez d'abord choisir un sport.\nSports disponibles :\n{sports_text}",
                "ar": f"من فضلك اختار الرياضة أولاً.\nالرياضات المتوفرة:\n{sports_text}",
            }
            send_friendly_message(phone, business, lang, retry_map.get(lang, retry_map["en"]), purpose="ask_sport")
            return "ok", 200
        state["selected_sport"] = chosen_sport
        state["step"] = "awaiting_service"
        services_text = format_service_bullets_for_sport(business["id"], chosen_sport)
        prompt_map = {
            "en": f"Great — {chosen_sport.capitalize()}. Which service would you like?\nAvailable services:\n{services_text}",
            "fr": f"Parfait — {chosen_sport.capitalize()}. Quel service souhaitez-vous ?\nServices disponibles :\n{services_text}",
            "ar": f"ممتاز — {chosen_sport.capitalize()}. أي خدمة بدك؟\nالخدمات المتوفرة:\n{services_text}",
        }
        send_friendly_message(phone, business, lang, prompt_map.get(lang, prompt_map["en"]), purpose="ask_service")
        return "ok", 200

    # STEP 2 – SERVICE (keywords + AI fallback
    if state and state.get("step") == "awaiting_service":
        lang = state.get("lang", lang)
        lt2 = t.lower()

        # User repeated a command instead of giving a service
        if is_booking_intent(t) or is_cancel_intent(t):
            send_friendly_message(
                phone,
                business,
                lang,
                tr(lang, "ask_service", name=state.get("name", "")),
                purpose="ask_service",
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

        valid_service, available_services = validate_service_for_business(
            business["id"], normalized
        )
        selected_sport = (state.get("selected_sport") or "").strip().lower()
        if valid_service and selected_sport:
            service_sport = (get_service_sport_category(business["id"], valid_service) or "").strip().lower()
            if service_sport and service_sport != selected_sport:
                valid_service = None

        if not valid_service:
            if selected_sport:
                services_text = format_service_bullets_for_sport(business["id"], selected_sport)
            elif available_services:
                services_text = "\n".join([f"• {s}" for s in available_services])
            else:
                services_text = "• No services configured yet"

            send_friendly_message(
                phone,
                business,
                lang,
                f"Sorry, we don’t offer that service.\nAvailable services:\n{services_text}",
                purpose="invalid_service",
            )
            return "ok", 200

        print("SERVICE STEP raw:", t, "ai:", ai_service, "normalized:", valid_service)

        state["service"] = valid_service
        state["step"] = "awaiting_date"

        send_friendly_message(
            phone,
            business,
            lang,
            tr(lang, "ask_date", service=valid_service),
            purpose="ask_date",
        )
        return "ok", 200

    # STEP 3 – DATE
    if state and state.get("step") == "awaiting_date":
        lang = state.get("lang", lang)

        if is_booking_intent(t) or is_cancel_intent(t):
            send_friendly_message(
                phone,
                business,
                lang,
                tr(lang, "ask_date", service=state.get("service", "")),
                purpose="ask_date",
            )
            return "ok", 200

        corrected_service, _sport, _available = resolve_valid_service_and_sport(business["id"], t, state.get("selected_sport"))
        if corrected_service:
            state["service"] = corrected_service
            send_friendly_message(phone, business, lang, tr(lang, "ask_date", service=corrected_service), purpose="ask_date")
            return "ok", 200

        try:
            normalized_date = normalize_booking_date(t)
        except Exception:
            send_friendly_message(phone, business, lang, tr(lang, "invalid_date"), purpose="ask_date")
            return "ok", 200

        if is_past_date_only(business, normalized_date):
            send_friendly_message(phone, business, lang, tr(lang, "past_date"), purpose="ask_date")
            return "ok", 200

        day_rules = get_day_rules(business["id"], normalized_date)
        if day_rules.get("closed"):
            send_friendly_message(phone, business, lang, tr(lang, "closed_day"), purpose="availability")
            return "ok", 200

        state["date"] = normalized_date
        state["step"] = "awaiting_time"

        send_friendly_message(phone, business, lang, tr(lang, "ask_time"), purpose="ask_time")
        return "ok", 200

    # STEP 4 – TIME
    if state and state.get("step") == "awaiting_time":
        lang = state.get("lang", lang)

        if is_booking_intent(t) or is_cancel_intent(t):
            send_friendly_message(phone, business, lang, tr(lang, "ask_time"), purpose="ask_time")
            return "ok", 200

        day_rules = get_day_rules(business["id"], state["date"])
        if day_rules.get("closed"):
            send_friendly_message(phone, business, lang, tr(lang, "closed_day"), purpose="availability")
            return "ok", 200

        time_ = normalize_time_str_with_hours(
            t,
            day_rules.get("open_time"),
            day_rules.get("close_time"),
        )

        if not time_:
            send_friendly_message(phone, business, lang, tr(lang, "invalid_time"), purpose="ask_time")
            return "ok", 200

        state["time"] = time_

        if not is_time_within_business_hours(time_, day_rules["open_time"], day_rules["close_time"]):
            send_friendly_message(phone, business, lang, tr(lang, "outside_hours"), purpose="availability")
            return "ok", 200

        eligible_resources = get_active_resources_for_service(business["id"], state["service"])

        # --------------------------------------------------
        # RESOURCE-BASED MODE
        # --------------------------------------------------
        if eligible_resources:
            requested_resource = extract_requested_resource_from_text(t, eligible_resources)
            preferred_resource = requested_resource or eligible_resources[0]

            preferred_rules = get_resource_day_rules(
                business["id"],
                preferred_resource["id"],
                state["date"],
            )

            preferred_available = (
                not preferred_rules.get("closed")
                and is_time_within_business_hours(
                    time_,
                    preferred_rules["open_time"],
                    preferred_rules["close_time"],
                )
                and not is_resource_slot_full(
                    business["id"],
                    preferred_resource["id"],
                    state["date"],
                    time_,
                    state["service"],
                )
            )

            if preferred_available:
                chosen_resource = preferred_resource

            else:
                same_time_options = [
                    r for r in get_available_resources_for_slot(
                        business["id"],
                        state["date"],
                        time_,
                        state["service"],
                    )
                    if r["id"] != preferred_resource["id"]
                ]

                nearby_with_preferred = suggest_slots_for_resource(
                    business["id"],
                    preferred_resource["id"],
                    state["date"],
                    time_,
                    state["service"],
                    max_suggestions=3,
                )

                # If another resource is available at the same time,
                # ask for confirmation instead of silently switching.
                if same_time_options:
                    offered_resource = same_time_options[0]
                    nearby_text = "\n".join([f"• {slot}" for slot in nearby_with_preferred]) if nearby_with_preferred else ""

                    state["step"] = "awaiting_alternative_confirmation"
                    state["alternative_offer"] = {
                        "preferred_resource_name": preferred_resource["name"],
                        "offered_resource_id": offered_resource["id"],
                        "offered_resource_name": offered_resource["name"],
                        "nearby_slots": nearby_with_preferred,
                    }

                    send_friendly_message(
                        phone,
                        business,
                        lang,
                        tr_switch_offer(
                            lang,
                            preferred_resource["name"],
                            offered_resource["name"],
                            state["date"],
                            time_,
                            nearby_text=nearby_text,
                        ),
                        purpose="slot_taken",
                    )
                    return "ok", 200

                # Otherwise only show nearby times with the preferred resource
                nearby_text = "\n".join([f"• {slot}" for slot in nearby_with_preferred]) if nearby_with_preferred else ""

                send_friendly_message(
                    phone,
                    business,
                    lang,
                    tr_switch_declined(lang, preferred_resource["name"], nearby_text),
                    purpose="slot_taken",
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
                    resource_id=chosen_resource["id"],
                    resource_name_snapshot=chosen_resource["name"],
                )
                print("Reservation saved with id:", reservation_id)

                gcal_event = add_reservation_to_google_calendar(
                    business["id"],
                    state.get("name", ""),
                    state.get("service", ""),
                    state.get("date", ""),
                    state.get("time", ""),
                    resource_name=chosen_resource["name"],
                    resource_id=chosen_resource["id"],
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
                    lang=lang,
                    resource_name=chosen_resource["name"],
                )

                user_state.pop(key, None)
                return "ok", 200

            except Exception as e:
                print("STEP 4 resource save error:", str(e))
                send_friendly_message(phone, business, lang, tr(lang, "save_error"), purpose="error")
                return "ok", 200

        # --------------------------------------------------
        # FALLBACK: old single-slot mode
        # --------------------------------------------------
        if is_slot_taken(business["id"], state["date"], time_, state["service"]):
            suggestions = suggest_slots(
                business["id"],
                state["date"],
                time_,
                state["service"],
                open_start=day_rules["open_time"],
                open_end=day_rules["close_time"],
                step_min=15,
                max_suggestions=3,
            )

            if suggestions:
                suggestions_text = "\n".join([f"• {s}" for s in suggestions])
                send_friendly_message(
                    phone,
                    business,
                    lang,
                    f"{tr(lang, 'slot_taken', date=state['date'], time=time_)}\n{suggestions_text}",
                    purpose="slot_taken",
                )
            else:
                send_friendly_message(
                    phone,
                    business,
                    lang,
                    tr(lang, "slot_taken", date=state["date"], time=time_),
                    purpose="slot_taken",
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
                business["id"],
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
                lang=lang,
            )

            user_state.pop(key, None)
            return "ok", 200

        except Exception as e:
            print("STEP 4 save error:", str(e))
            send_friendly_message(phone, business, lang, tr(lang, "save_error"), purpose="error")
            return "ok", 200

def calculate_dashboard_metrics(business, reservations):
    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")
    today_iso = datetime.now(tz).date().isoformat()

    metrics = {
        "today_booked_revenue": 0.0,
        "today_done_revenue": 0.0,
    }

    for r in reservations:
        if r["date"] != today_iso:
            continue

        price = get_effective_service_price_for_reservation_row(business, r)

        if r["status"] == "CONFIRMED":
            metrics["today_booked_revenue"] += price
        elif r["status"] == "DONE":
            metrics["today_done_revenue"] += price

    return metrics

def get_services_for_business(business_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, name, price, duration_min, sport_category, night_price, capacity_units_used
        FROM services
        WHERE business_id = %s
        ORDER BY id DESC
        """,
        (business_id,),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def get_resources_for_business(business_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT
            r.id,
            r.name,
            r.resource_type,
            r.capacity,
            r.is_active,
            r.display_order,
            r.color_tag,
            COUNT(rs.id) AS assigned_services_count
        FROM resources r
        LEFT JOIN resource_services rs ON rs.resource_id = r.id
        WHERE r.business_id = %s
        GROUP BY r.id
        ORDER BY r.display_order ASC, r.id ASC
        """,
        (business_id,),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def get_resource_services_map(business_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT resource_id, service_id
        FROM resource_services
        WHERE business_id = %s
        """,
        (business_id,),
    )
    rows = c.fetchall()
    conn.close()

    mapping = {}
    for row in rows:
        mapping.setdefault(row["resource_id"], set()).add(row["service_id"])
    return mapping


def ensure_default_resource_hours(resource_id, business_id):
    """
    When a new resource is created, copy the business weekly schedule
    into resource_hours so every staff/court starts with the business defaults.
    """
    ensure_default_hours(business_id)

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT weekday, is_closed, open_time, close_time
        FROM business_hours
        WHERE business_id = %s
        ORDER BY weekday
        """,
        (business_id,),
    )
    rows = c.fetchall()

    for row in rows:
        c.execute(
            """
            INSERT INTO resource_hours (business_id, resource_id, weekday, is_closed, open_time, close_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (resource_id, weekday) DO NOTHING
            """,
            (
                business_id,
                resource_id,
                row["weekday"],
                row["is_closed"],
                row["open_time"],
                row["close_time"],
            ),
        )

    conn.commit()
    conn.close()


def get_resource_by_id(resource_id, business_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT *
        FROM resources
        WHERE id = %s AND business_id = %s
        LIMIT 1
        """,
        (resource_id, business_id),
    )
    row = c.fetchone()
    conn.close()
    return row

def get_resource_calendar_color_id(business_id, resource_id=None, resource_name=None):
    """
    Returns a Google Calendar event color_id string based on resource.color_tag.
    """
    resource = None

    if resource_id is not None:
        resource = get_resource_by_id(resource_id, business_id)

    elif resource_name:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            """
            SELECT *
            FROM resources
            WHERE business_id = %s
              AND lower(trim(name)) = lower(trim(%s))
            LIMIT 1
            """,
            (business_id, resource_name),
        )
        resource = c.fetchone()
        conn.close()

    if not resource:
        return None

    tag = (resource.get("color_tag") or "").strip().lower()

    color_map = {
        "lavender": "1",
        "sage": "2",
        "purple": "3",
        "pink": "4",
        "yellow": "5",
        "orange": "6",
        "teal": "7",
        "gray": "8",
        "blue": "9",
        "green": "10",
        "red": "11",
    }

    return color_map.get(tag)

def normalize_capacity(value, default=1):
    try:
        n = int(value)
        return n if n >= 1 else default
    except Exception:
        return default

def is_support_user():
    return session.get("role") == "support"


def require_login():
    return "user_id" in session


def require_support():
    return "user_id" in session and session.get("role") == "support"

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

    message_id = message.get("id")

    if is_message_already_done(message_id):
        print("Duplicate message already completed:", message_id, flush=True)
        return "ok", 200

    if is_message_currently_processing(message_id):
        print("Duplicate message still processing:", message_id, flush=True)
        return "ok", 200

    mark_message_processing(message_id)

    phone = message.get("from")
    text = message.get("text", {}).get("body", "").strip()
    phone_number_id = value["metadata"]["phone_number_id"]
    print("phone:", phone, "text:", text, "phone_number_id:", phone_number_id, flush=True)

    business = get_business_by_phone_number_id(phone_number_id)
    print("business lookup result:", dict(business) if business else None, flush=True)
    if not business:
        clear_message_processing(message_id)
        print("No business configured for phone_number_id", phone_number_id, flush=True)
        return "ok", 200

    print("Calling process_incoming_message...", flush=True)

    try:
        result = process_incoming_message(business, phone, text)
        mark_message_done(message_id)
        return result
    except Exception as e:
        clear_message_processing(message_id)
        print("process_incoming_message error:", str(e), flush=True)
        return "ok", 200

# ------------------ ADMIN BUSINESSES ------------------ #


from flask import render_template_string, request  # make sure this is imported at the top

@app.route("/admin/businesses", methods=["GET", "POST"])
def admin_businesses():
    if not require_support():
        return redirect("/login")

    conn = get_db_connection()
    c = conn.cursor()

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        provider = request.form.get("provider", "meta").strip().lower()
        phone_number_id = request.form.get("phone_number_id", "").strip()
        access_token = request.form.get("access_token", "").strip()
        calendar_id = request.form.get("calendar_id", "primary").strip()
        timezone = request.form.get("timezone", "Asia/Beirut").strip()

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

        conn.close()
        return redirect("/admin/businesses")

    search = request.args.get("q", "").strip()

    if search:
        like = f"%{search.lower()}%"
        c.execute(
            """
            SELECT id, name, provider, phone_number_id, timezone, access_token, gcal_credentials
            FROM businesses
            WHERE lower(name) LIKE %s
               OR CAST(id AS TEXT) LIKE %s
               OR lower(provider) LIKE %s
            ORDER BY id DESC
            """,
            (like, like, like),
        )
    else:
        c.execute(
            """
            SELECT id, name, provider, phone_number_id, timezone, access_token, gcal_credentials
            FROM businesses
            ORDER BY id DESC
            """
        )

    businesses = c.fetchall()
    conn.close()

    total_businesses = len(businesses)
    whatsapp_active = sum(1 for b in businesses if b.get("access_token"))
    calendar_connected = sum(1 for b in businesses if b.get("gcal_credentials"))

    return render_template(
        "admin_businesses.html",
        businesses=businesses,
        search=search,
        total_businesses=total_businesses,
        whatsapp_active=whatsapp_active,
        calendar_connected=calendar_connected,
    )


# ------------------ ADMIN SERVICES ------------------


@app.route("/admin/<int:business_id>/services", methods=["GET", "POST"])
def admin_services(business_id):
    if not require_support():
        return redirect("/login")
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
        SELECT id, name, price, duration_min, sport_category, night_price, capacity_units_used
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
      <a href="/dashboard?business_id={{ business_id }}">Back to dashboard</a> |
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
    if request.method == "GET":
        return render_template("register.html")

    business_name = (request.form.get("business_name") or "").strip()
    username = (request.form.get("username") or "").strip().lower()
    password = (request.form.get("password") or "").strip()

    if not business_name or not username or not password:
        return render_template("register.html", error="Please fill in all fields.")

    conn = get_db_connection()
    c = conn.cursor()

    try:
        c.execute("SELECT id FROM users WHERE email = %s", (username,))
        existing_user = c.fetchone()
        if existing_user:
            conn.close()
            return render_template("register.html", error="This username is already taken.")

        c.execute(
            """
            INSERT INTO businesses (name, calendar_id, timezone)
            VALUES (%s, 'primary', 'Asia/Beirut')
            RETURNING id
            """,
            (business_name,),
        )
        business = c.fetchone()
        business_id = business["id"]

        password_hash = generate_password_hash(password)
        c.execute(
            """
            INSERT INTO users (email, password_hash, business_id)
            VALUES (%s, %s, %s)
            """,
            (username, password_hash, business_id),
        )

        conn.commit()
        session["business_id"] = business_id
        return redirect("/dashboard?tab=settings")

    except Exception as e:
        conn.rollback()
        print("register error:", str(e))
        return render_template("register.html", error="Something went wrong while creating the account.")
    finally:
        conn.close()


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "").strip()

        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            """
            SELECT id, business_id, password_hash, role
            FROM users
            WHERE email = %s
            """,
            (username,),
        )
        user = c.fetchone()
        conn.close()

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["business_id"] = user["business_id"]
            session["role"] = user.get("role", "business")
            return redirect("/dashboard")

        return render_template("login.html", error="Invalid username or password.")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


# ------------------ DASHBOARD + CONFIRM / CANCEL ------------------




@app.route("/shop-mode")
def shop_mode():
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    business = get_business_by_id(business_id)
    if not business:
        return redirect("/login")

    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")
    now_dt = datetime.now(tz)
    today_iso = now_dt.date().isoformat()

    try:
        mark_past_reservations_done(business)
    except Exception as e:
        print("shop_mode mark_past_reservations_done warning:", e, flush=True)

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, customer_name, customer_phone, service, date, time,
               status, resource_name_snapshot
        FROM reservations
        WHERE business_id = %s
          AND date = %s
          AND status = 'CONFIRMED'
        ORDER BY time ASC, id ASC
        """,
        (business_id, today_iso),
    )
    reservations = c.fetchall()
    conn.close()

    next_reservation = None
    now_hhmm = now_dt.strftime("%H:%M")

    enriched = []
    for r in reservations:
        item = dict(r)
        item["is_next"] = False
        if next_reservation is None and item["time"] >= now_hhmm:
            item["is_next"] = True
            next_reservation = item
        enriched.append(item)

    stats = {
        "total": len(enriched),
        "confirmed": len(enriched),
    }

    return render_template(
        "shop_mode.html",
        business=business,
        reservations=enriched,
        next_reservation=next_reservation,
        stats=stats,
        now_display=now_dt.strftime("%H:%M"),
        today_display=now_dt.strftime("%A %d %B %Y"),
    )

@app.route("/dashboard")
def dashboard():
    if not require_login():
        return redirect("/login")

    ensure_fb_tables()
    ensure_business_feature_columns()
    ensure_reservation_extension_columns()
    ensure_resource_availability_tables()
    ensure_service_metadata_columns()

    requested_business_id = request.args.get("business_id", type=int)

    if is_support_user() and requested_business_id:
        business_id = requested_business_id
    else:
        business_id = session.get("business_id")

    conn = get_db_connection()
    c = conn.cursor()

    c.execute("SELECT * FROM businesses WHERE id = %s", (business_id,))
    business = c.fetchone()
    if not business:
        conn.close()
        return f"No business with ID {business_id}", 404

    session["business_id"] = business_id

    feature_flags = get_business_feature_flags(business)
    business["enable_fb"] = feature_flags["enable_fb"]
    business["enable_resource_blocking"] = feature_flags["enable_resource_blocking"]
    business["enable_time_extension"] = feature_flags["enable_time_extension"]
    business["extension_pricing_mode"] = feature_flags["extension_pricing_mode"]
    business["extension_flat_30_price"] = feature_flags["extension_flat_30_price"]

    ensure_default_hours(business_id)

    try:
        mark_past_reservations_done(business)
    except Exception as e:
        print("dashboard mark_past_reservations_done warning:", e)

    tz = pytz.timezone(business.get("timezone") or "Asia/Beirut")
    now = datetime.now(tz)
    cutoff_dt = now - timedelta(hours=48)
    cutoff_date_iso = cutoff_dt.date().isoformat()

    search_query = (request.args.get("q") or "").strip().lower()
    status_filter = (request.args.get("status") or "ALL").strip().upper()
    toast = (request.args.get("toast") or "").strip()
    toast_type = (request.args.get("toast_type") or "info").strip().lower()

    # Load services once
    c.execute(
        """
        SELECT id, name, price, duration_min, sport_category, night_price, capacity_units_used
        FROM services
        WHERE business_id = %s
        ORDER BY id DESC
        """,
        (business_id,),
    )
    services = [dict(s) for s in c.fetchall()]
    for s in services:
        if not s.get("sport_category"):
            s["sport_category"] = infer_service_sport_from_name(s.get("name"))
        if s.get("night_price") is None:
            inferred_night = SPECIAL_NIGHT_PRICE_MAP.get((s.get("name") or "").strip().lower())
            if inferred_night is not None:
                s["night_price"] = inferred_night
        if not s.get("capacity_units_used"):
            s["capacity_units_used"] = infer_service_capacity_units_from_name(s.get("name"))

    service_meta = {}
    for s in services:
        service_meta[(s["name"] or "").strip().lower()] = {
            "price": float(s.get("price") or 0),
            "duration": int(s.get("duration_min") or 45),
        }

    def get_service_meta(service_name):
        cleaned = (service_name or "").strip().lower()

        if cleaned in service_meta:
            return service_meta[cleaned]

        for name, meta in service_meta.items():
            if cleaned in name or name in cleaned:
                return meta

        return {"price": 0.0, "duration": 45}

    # Load resources for future staff/court UI
    c.execute(
        """
        SELECT
            r.id,
            r.name,
            r.resource_type,
            r.capacity,
            r.is_active,
            r.display_order,
            r.color_tag,
            COUNT(rs.id) AS assigned_services_count
        FROM resources r
        LEFT JOIN resource_services rs ON rs.resource_id = r.id
        WHERE r.business_id = %s
        GROUP BY r.id
        ORDER BY r.display_order ASC, r.id ASC
        """,
        (business_id,),
    )
    resources = c.fetchall()

    c.execute(
        """
        SELECT resource_id, service_id
        FROM resource_services
        WHERE business_id = %s
        """,
        (business_id,),
    )
    resource_service_rows = c.fetchall()

    resource_services_map = {}
    for row in resource_service_rows:
        resource_services_map.setdefault(row["resource_id"], set()).add(row["service_id"])

    # Load only recent/future reservations
    c.execute(
        """
        SELECT id, customer_name, customer_phone, service, date, time, status, notes, resource_id, resource_name_snapshot,
               COALESCE(extra_minutes, 0) AS extra_minutes,
               COALESCE(extra_price, 0) AS extra_price,
               COALESCE(extra_minutes, 0) AS extra_minutes,
               COALESCE(extra_price, 0) AS extra_price
        FROM reservations
        WHERE business_id = %s
          AND date >= %s
        ORDER BY date DESC, time DESC, id DESC
        """,
        (business_id, cutoff_date_iso),
    )
    recent_reservations = c.fetchall()

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
        ORDER BY blocked_date DESC
        """,
        (business_id,),
    )
    blocked_dates = c.fetchall()

    c.execute(
        """
        SELECT id, resource_id, blocked_date::text AS blocked_date, COALESCE(note, '') AS note
        FROM resource_blocked_dates
        WHERE business_id = %s
        ORDER BY blocked_date DESC, id DESC
        """,
        (business_id,),
    )
    resource_blocked_rows = c.fetchall()

    c.execute(
        """
        SELECT id, customer_name, customer_phone, service, date, time, status, notes, resource_id, resource_name_snapshot,
               COALESCE(extra_minutes, 0) AS extra_minutes,
               COALESCE(extra_price, 0) AS extra_price
        FROM reservations
        WHERE business_id = %s
        ORDER BY date DESC, time DESC, id DESC
        """,
        (business_id,),
    )
    all_reservations = c.fetchall()

    c.execute(
        """
        SELECT id, business_id, name, price, is_active, created_at, updated_at
        FROM fb_products
        WHERE business_id = %s
        ORDER BY id ASC
        """,
        (business_id,),
    )
    fb_products = c.fetchall()

    conn.close()

    visible_reservations = []

    for r in recent_reservations:
        try:
            normalized_time = normalize_time_str(r["time"]) or r["time"]
            start_naive = datetime.strptime(
                f"{r['date']} {normalized_time}",
                "%Y-%m-%d %H:%M"
            )
            start_dt = tz.localize(start_naive)

            duration_min = get_service_meta(r["service"])["duration"] + int(r.get("extra_minutes") or 0)
            end_dt = start_dt + timedelta(minutes=duration_min)

            if end_dt >= cutoff_dt:
                visible_reservations.append(r)

        except Exception as e:
            print("dashboard reservation filter warning:", r, e)
            visible_reservations.append(r)

    def reservation_sort_key(r):
        try:
            normalized_time = normalize_time_str(r["time"]) or r["time"]
            dt = datetime.strptime(
                f"{r['date']} {normalized_time}",
                "%Y-%m-%d %H:%M"
            )
            return tz.localize(dt)
        except Exception:
            return tz.localize(datetime(2000, 1, 1, 0, 0))

    reservations = sorted(
        visible_reservations,
        key=reservation_sort_key,
        reverse=True
    )

    filtered_reservations = reservations

    if search_query:
        filtered_reservations = [
            r for r in filtered_reservations
            if search_query in (r.get("customer_name") or "").lower()
            or search_query in (r.get("customer_phone") or "").lower()
            or search_query in (r.get("service") or "").lower()
            or search_query in (r.get("notes") or "").lower()
            or search_query in (r.get("resource_name_snapshot") or "").lower()
        ]

    if status_filter and status_filter != "ALL":
        filtered_reservations = [
            r for r in filtered_reservations
            if (r.get("status") or "").upper() == status_filter
        ]

    today_iso = now.date().isoformat()
    dashboard_metrics = {
        "today_booked_revenue": 0.0,
        "today_done_revenue": 0.0,
    }

    for r in reservations:
        if r["date"] != today_iso:
            continue

        price = get_effective_service_price_for_reservation_row(business, r) + float(r.get("extra_price") or 0)

        if r["status"] == "CONFIRMED":
            dashboard_metrics["today_booked_revenue"] += price
        elif r["status"] == "DONE":
            dashboard_metrics["today_done_revenue"] += price

    today_reservations = sorted(
        [
            r for r in reservations
            if r["date"] == today_iso and r["status"] in ["CONFIRMED", "DONE"]
        ],
        key=reservation_sort_key
    )

    resource_blocked_map = {}
    for row in resource_blocked_rows:
        resource_blocked_map.setdefault(row["resource_id"], []).append(row)

    same_day_confirmed_map = {}
    for row in all_reservations:
        if (row.get("status") or "").upper() != "CONFIRMED":
            continue
        same_day_confirmed_map.setdefault(row["date"], []).append(row)

    for r in filtered_reservations:
        r["extra_minutes"] = int(r.get("extra_minutes") or 0)
        r["extra_price"] = float(r.get("extra_price") or 0)
        r["can_mark_done"] = reservation_has_ended(business, r) and (r.get("status") or "").upper() == "CONFIRMED"
        if feature_flags.get("enable_time_extension") and (r.get("status") or "").upper() == "CONFIRMED":
            r["can_add_30"] = is_reservation_extension_possible(business, r, increment_minutes=30)[0]
        else:
            r["can_add_30"] = False

    report_metrics = compute_dashboard_report_metrics(business, services, all_reservations)
    fb_report_metrics = compute_fb_report_metrics(business)
    report_metrics.update(fb_report_metrics)
    report_metrics["weekly_total_revenue"] = (
        float(report_metrics.get("weekly_reservations_revenue") or 0)
        + float(report_metrics.get("weekly_fb_revenue") or 0)
    )
    fb_recent_sales = get_fb_recent_sales(business, limit=10)

    return render_template(
        "dashboard.html",
        business=business,
        reservations=filtered_reservations,
        services=services,
        service_options=services,
        resources=resources,
        resource_services_map=resource_services_map,
        resource_blocked_map=resource_blocked_map,
        hours=hours,
        blocked_dates=blocked_dates,
        fb_products=fb_products,
        fb_recent_sales=fb_recent_sales,
        weekday_names=WEEKDAY_NAMES,
        active_tab=request.args.get("tab", "reservations"),
        google_calendar_connected=should_attempt_calendar_sync(business),
        whatsapp_connected=bool(business.get("access_token")),
        is_support=is_support_user(),
        dashboard_metrics=dashboard_metrics,
        today_reservations=today_reservations,
        search_query=search_query,
        status_filter=status_filter or "ALL",
        toast=toast,
        toast_type=toast_type,
        report_metrics=report_metrics,
        feature_flags=feature_flags,
        feature_fb=feature_flags["enable_fb"],
        feature_resource_blocking=feature_flags["enable_resource_blocking"],
        feature_time_extension=feature_flags["enable_time_extension"],
    )

@app.route("/cancel/<int:reservation_id>")
def cancel_reservation(reservation_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT customer_phone, customer_name, service, date, time, google_event_id, status
        FROM reservations
        WHERE id = %s AND business_id = %s
        """,
        (reservation_id, business_id),
    )
    row = c.fetchone()

    if not row:
        conn.close()
        return redirect("/dashboard")

    status = row["status"]

    # If already canceled or done, do nothing
    if status != "CONFIRMED":
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
            deleted = delete_event(google_event_id, calendar_id=(business.get("calendar_id") or "primary"))
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


@app.route("/resources/block/<int:resource_id>", methods=["POST"])
def block_resource_date(resource_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    business = get_business_by_id(business_id)
    if not business:
        return dashboard_redirect_with_toast("Business not found.", "error", tab="resources")

    ensure_business_feature_columns()
    ensure_resource_availability_tables()
    if not get_business_feature_flags(business).get("enable_resource_blocking"):
        return dashboard_redirect_with_toast("Resource blocking is disabled for this business.", "error", tab="resources")

    blocked_date_raw = (request.form.get("blocked_date") or "").strip()
    note = (request.form.get("note") or "").strip()
    if not blocked_date_raw:
        return dashboard_redirect_with_toast("Please choose a date to block.", "error", tab="resources")

    blocked_date = normalize_booking_date(blocked_date_raw)

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id FROM resource_blocked_dates
        WHERE business_id = %s AND resource_id = %s AND blocked_date = %s
        LIMIT 1
        """,
        (business_id, resource_id, blocked_date),
    )
    existing = c.fetchone()
    if existing:
        c.execute(
            """
            UPDATE resource_blocked_dates
            SET note = %s
            WHERE id = %s AND business_id = %s
            """,
            (note, existing["id"], business_id),
        )
    else:
        c.execute(
            """
            INSERT INTO resource_blocked_dates (business_id, resource_id, blocked_date, note)
            VALUES (%s, %s, %s, %s)
            """,
            (business_id, resource_id, blocked_date, note),
        )
    conn.commit()
    conn.close()

    return dashboard_redirect_with_toast("Resource blocked successfully.", "success", tab="resources")


@app.route("/resources/unblock/<int:block_id>", methods=["POST"])
def unblock_resource_date(block_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        DELETE FROM resource_blocked_dates
        WHERE id = %s AND business_id = %s
        """,
        (block_id, business_id),
    )
    conn.commit()
    conn.close()

    return dashboard_redirect_with_toast("Blocked date removed.", "success", tab="resources")


@app.route("/reservations/mark-done/<int:reservation_id>", methods=["POST"])
def mark_reservation_done(reservation_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    business = get_business_by_id(business_id)
    if not business:
        return dashboard_redirect_with_toast("Business not found.", "error", "reservations")

    ensure_reservation_extension_columns()

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, date, time, service, status, COALESCE(extra_minutes, 0) AS extra_minutes
        FROM reservations
        WHERE id = %s AND business_id = %s
        LIMIT 1
        """,
        (reservation_id, business_id),
    )
    reservation = c.fetchone()
    if not reservation:
        conn.close()
        return dashboard_redirect_with_toast("Reservation not found.", "error", "reservations")

    if (reservation.get("status") or "").upper() != "CONFIRMED":
        conn.close()
        return dashboard_redirect_with_toast("Only confirmed reservations can be marked done.", "error", "reservations")

    if not reservation_has_ended(business, reservation):
        conn.close()
        return dashboard_redirect_with_toast("This reservation has not ended yet.", "error", "reservations")

    c.execute(
        """
        UPDATE reservations
        SET status = 'DONE'
        WHERE id = %s AND business_id = %s
        """,
        (reservation_id, business_id),
    )
    conn.commit()
    conn.close()

    return dashboard_redirect_with_toast("Reservation marked as done.", "success", "reservations")


@app.route("/settings/update", methods=["POST"])
def update_business_settings():
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    business_name = request.form.get("business_name", "").strip()
    timezone = request.form.get("timezone", "Asia/Beirut").strip() or "Asia/Beirut"
    preferred_language = request.form.get("preferred_language", "auto").strip().lower() or "auto"
    assistant_tone = request.form.get("assistant_tone", "friendly").strip().lower() or "friendly"
    custom_welcome_message = request.form.get("custom_welcome_message", "").strip()
    business_description = request.form.get("business_description", "").strip()

    ensure_business_feature_columns()
    submitted_feature_fields = {
        "enable_fb",
        "enable_resource_blocking",
        "enable_time_extension",
        "extension_pricing_mode",
        "extension_flat_30_price",
    }

    conn = get_db_connection()
    c = conn.cursor()

    updates = [
        "name = %s",
        "timezone = %s",
        "preferred_language = %s",
        "assistant_tone = %s",
        "custom_welcome_message = %s",
        "business_description = %s",
    ]
    params = [
        business_name,
        timezone,
        preferred_language,
        assistant_tone,
        custom_welcome_message,
        business_description,
    ]

    if submitted_feature_fields.intersection(set(request.form.keys())):
        updates.extend([
            "enable_fb = %s",
            "enable_resource_blocking = %s",
            "enable_time_extension = %s",
            "extension_pricing_mode = %s",
            "extension_flat_30_price = %s",
        ])
        params.extend([
            True if request.form.get("enable_fb") == "on" else False,
            True if request.form.get("enable_resource_blocking") == "on" else False,
            True if request.form.get("enable_time_extension") == "on" else False,
            (request.form.get("extension_pricing_mode") or "tier_diff").strip().lower() or "tier_diff",
            safe_float(request.form.get("extension_flat_30_price") or 0, 0.0),
        ])

    params.append(business_id)

    c.execute(
        f"""
        UPDATE businesses
        SET {", ".join(updates)}
        WHERE id = %s
        """,
        tuple(params),
    )
    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=settings")


@app.route("/services/add", methods=["POST"])
def add_service():
    if "business_id" not in session:
        return redirect("/login")
    ensure_service_metadata_columns()
    business_id = session["business_id"]
    name = request.form.get("name", "").strip()
    price = float(request.form.get("price") or 0)
    duration_min = int(request.form.get("duration_min") or 45)
    sport_category = (request.form.get("sport_category") or infer_service_sport_from_name(name) or "").strip().lower() or None
    night_price_raw = request.form.get("night_price")
    night_price = safe_float(night_price_raw, 0.0) if str(night_price_raw or "").strip() != "" else None
    capacity_units_used = max(1, safe_int(request.form.get("capacity_units_used") or infer_service_capacity_units_from_name(name), 1))
    if name:
        conn = get_db_connection(); c = conn.cursor()
        c.execute("""INSERT INTO services (name, price, duration_min, business_id, sport_category, night_price, capacity_units_used)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                  (name, price, duration_min, business_id, sport_category, night_price, capacity_units_used))
        conn.commit(); conn.close()
    return redirect("/dashboard?tab=services")

@app.route("/services/update/<int:service_id>", methods=["POST"])
def update_service(service_id):
    if "business_id" not in session:
        return redirect("/login")
    ensure_service_metadata_columns()
    business_id = session["business_id"]
    name = (request.form.get("name") or "").strip()
    price = safe_float(request.form.get("price") or 0, 0.0)
    duration_min = safe_int(request.form.get("duration_min") or 45, 45)
    sport_category = (request.form.get("sport_category") or infer_service_sport_from_name(name) or "").strip().lower() or None
    night_price_raw = request.form.get("night_price")
    night_price = safe_float(night_price_raw, 0.0) if str(night_price_raw or "").strip() != "" else None
    capacity_units_used = max(1, safe_int(request.form.get("capacity_units_used") or infer_service_capacity_units_from_name(name), 1))
    conn = get_db_connection(); c = conn.cursor()
    c.execute("""UPDATE services
                 SET name=%s, price=%s, duration_min=%s, sport_category=%s, night_price=%s, capacity_units_used=%s
                 WHERE id=%s AND business_id=%s""",
              (name, price, duration_min, sport_category, night_price, capacity_units_used, service_id, business_id))
    conn.commit(); conn.close()
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

@app.route("/resources/add", methods=["POST"])
def add_resource():
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    name = (request.form.get("name") or "").strip()
    resource_type = (request.form.get("resource_type") or "staff").strip().lower()
    capacity = normalize_capacity(request.form.get("capacity"), default=1)
    color_tag = (request.form.get("color_tag") or "").strip().lower() or None

    if not name:
        return dashboard_redirect_with_toast("Resource availability updated.", "success", "resources")

    conn = get_db_connection()
    c = conn.cursor()

    try:
        c.execute(
            """
            INSERT INTO resources (business_id, name, resource_type, capacity, is_active, display_order, color_tag)
            VALUES (%s, %s, %s, %s, TRUE, 0, %s)
            RETURNING id
            """,
            (business_id, name, resource_type, capacity, color_tag),
        )
        row = c.fetchone()
        resource_id = row["id"]

        conn.commit()
        conn.close()

        ensure_default_resource_hours(resource_id, business_id)

    except Exception as e:
        conn.rollback()
        conn.close()
        print("add_resource error:", str(e))

    return redirect("/dashboard?tab=resources")


@app.route("/resources/delete/<int:resource_id>", methods=["POST"])
def delete_resource(resource_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        DELETE FROM resources
        WHERE id = %s AND business_id = %s
        """,
        (resource_id, business_id),
    )
    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=resources")


@app.route("/resources/toggle/<int:resource_id>", methods=["POST"])
def toggle_resource(resource_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        UPDATE resources
        SET is_active = NOT is_active
        WHERE id = %s AND business_id = %s
        """,
        (resource_id, business_id),
    )
    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=resources")


@app.route("/resources/update/<int:resource_id>", methods=["POST"])
def update_resource(resource_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    name = (request.form.get("name") or "").strip()
    resource_type = (request.form.get("resource_type") or "staff").strip().lower()
    capacity = normalize_capacity(request.form.get("capacity"), default=1)
    color_tag = (request.form.get("color_tag") or "").strip().lower() or None

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        UPDATE resources
        SET name = %s,
            resource_type = %s,
            capacity = %s,
            color_tag = %s
        WHERE id = %s AND business_id = %s
        """,
        (name, resource_type, capacity, color_tag, resource_id, business_id),
    )
    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=resources")


@app.route("/resources/assign-services/<int:resource_id>", methods=["POST"])
def assign_resource_services(resource_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    selected_service_ids = request.form.getlist("service_ids")

    clean_ids = []
    for value in selected_service_ids:
        try:
            clean_ids.append(int(value))
        except Exception:
            pass

    conn = get_db_connection()
    c = conn.cursor()

    try:
        c.execute(
            """
            DELETE FROM resource_services
            WHERE resource_id = %s AND business_id = %s
            """,
            (resource_id, business_id),
        )

        for service_id in clean_ids:
            c.execute(
                """
                INSERT INTO resource_services (business_id, resource_id, service_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (resource_id, service_id) DO NOTHING
                """,
                (business_id, resource_id, service_id),
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("assign_resource_services error:", str(e))
    finally:
        conn.close()

    return redirect("/dashboard?tab=resources")

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

@app.route("/reservations/add-manual", methods=["POST"])
def add_manual_reservation():
    return _handle_manual_add_reservation()



@app.route("/reservations/reschedule/<int:reservation_id>", methods=["POST"])
def reschedule_reservation(reservation_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    business = get_business_by_id(business_id)
    if not business:
        return dashboard_redirect_with_toast("Business not found.", "error")

    new_date = (request.form.get("date") or "").strip()
    new_time_raw = (request.form.get("time") or "").strip()
    new_resource_raw = (request.form.get("resource_id") or "").strip()

    if not new_date or not new_time_raw:
        return dashboard_redirect_with_toast("Please provide a new date and time.", "error")

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, customer_name, customer_phone, service, status, google_event_id,
               resource_id, resource_name_snapshot
        FROM reservations
        WHERE id = %s AND business_id = %s
        LIMIT 1
        """,
        (reservation_id, business_id),
    )
    reservation = c.fetchone()
    conn.close()

    if not reservation:
        return dashboard_redirect_with_toast("Reservation not found.", "error")

    if (reservation.get("status") or "").upper() == "CANCELED":
        return dashboard_redirect_with_toast("Canceled reservations cannot be rescheduled.", "error")

    valid_service, _ = validate_service_for_business(business_id, reservation["service"])
    if not valid_service:
        return dashboard_redirect_with_toast("Service is no longer valid for this business.", "error")

    day_rules = get_day_rules(business_id, new_date)
    if day_rules.get("closed"):
        return dashboard_redirect_with_toast("This date is closed for reservations.", "error")

    normalized_time = normalize_time_str_with_hours(
        new_time_raw,
        day_rules.get("open_time"),
        day_rules.get("close_time"),
    )
    if not normalized_time:
        return dashboard_redirect_with_toast("Please choose a valid time.", "error")

    if is_past_reservation_datetime(business, new_date, normalized_time):
        return dashboard_redirect_with_toast("You cannot reschedule to a past date or time.", "error")

    if not is_time_within_business_hours(normalized_time, day_rules["open_time"], day_rules["close_time"]):
        return dashboard_redirect_with_toast("That time is outside business hours.", "error")

    reservations_rows = get_confirmed_reservations_for_date_excluding_fast(
        business_id,
        new_date,
        excluded_reservation_id=reservation_id,
    )
    eligible_resources = get_active_resources_for_service(business_id, valid_service)
    service_duration_cache = build_service_duration_cache(
        business_id,
        reservations_rows,
        extra_service_names=[valid_service],
    )

    selected_resource_raw = new_resource_raw or (str(reservation.get("resource_id")) if reservation.get("resource_id") else "auto")
    chosen_resource = None

    if eligible_resources:
        chosen_resource, error_message = get_manual_reservation_resource_choice_fast(
            business_id,
            valid_service,
            selected_resource_raw,
            new_date,
            normalized_time,
            eligible_resources,
            reservations_rows,
            service_duration_cache,
        )
        if error_message:
            return dashboard_redirect_with_toast(error_message, "error")
    else:
        new_duration = int(service_duration_cache.get(valid_service, 45))
        new_start = time_to_minutes(normalized_time)
        for row in reservations_rows:
            existing_time = normalize_time_str(row["time"])
            if not existing_time:
                continue
            existing_start = time_to_minutes(existing_time)
            existing_duration = int(service_duration_cache.get(row["service"], 45))
            if ranges_overlap(new_start, new_duration, existing_start, existing_duration):
                return dashboard_redirect_with_toast("This time slot is already taken.", "error")

    apply_reschedule_update(
        business,
        reservation,
        new_date,
        normalized_time,
        chosen_resource,
    )

    if reservation.get("customer_phone"):
        try:
            send_reservation_rescheduled(
                reservation.get("customer_phone"),
                reservation.get("customer_name", ""),
                valid_service,
                reservation.get("date", ""),
                reservation.get("time", ""),
                new_date,
                normalized_time,
                business,
                old_resource_name=reservation.get("resource_name_snapshot"),
                new_resource_name=chosen_resource["name"] if chosen_resource else None,
            )
        except Exception as e:
            print("reschedule whatsapp notify warning:", e, flush=True)

    return dashboard_redirect_with_toast("Reservation rescheduled successfully.", "success")

@app.route("/reservations/update-note/<int:reservation_id>", methods=["POST"])
def update_reservation_note(reservation_id):
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]
    note = (request.form.get("note") or "").strip()

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        UPDATE reservations
        SET notes = %s
        WHERE id = %s AND business_id = %s
        """,
        (note, reservation_id, business_id),
    )
    conn.commit()
    conn.close()

    return redirect("/dashboard?tab=reservations")


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


@app.route("/fb/products/add", methods=["POST"])
def add_fb_product():
    if "business_id" not in session:
        return redirect("/login")

    ensure_fb_tables()
    business_id = session["business_id"]
    name = (request.form.get("name") or "").strip()
    price = safe_float(request.form.get("price") or 0, 0.0)

    if not name:
        return dashboard_redirect_with_toast("Please enter a product name.", "error", "fb")

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO fb_products (business_id, name, price, is_active)
        VALUES (%s, %s, %s, TRUE)
        """,
        (business_id, name, price),
    )
    conn.commit()
    conn.close()

    return dashboard_redirect_with_toast("F&B product added successfully.", "success", "fb")


@app.route("/fb/products/update/<int:product_id>", methods=["POST"])
def update_fb_product(product_id):
    if "business_id" not in session:
        return redirect("/login")

    ensure_fb_tables()
    business_id = session["business_id"]
    name = (request.form.get("name") or "").strip()
    price = safe_float(request.form.get("price") or 0, 0.0)
    is_active = request.form.get("is_active") == "on"

    if not name:
        return dashboard_redirect_with_toast("Product name cannot be empty.", "error", "fb")

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        UPDATE fb_products
        SET name = %s,
            price = %s,
            is_active = %s,
            updated_at = NOW()
        WHERE id = %s AND business_id = %s
        """,
        (name, price, is_active, product_id, business_id),
    )
    conn.commit()
    conn.close()

    return dashboard_redirect_with_toast("F&B product updated successfully.", "success", "fb")


@app.route("/fb/products/delete/<int:product_id>", methods=["POST"])
def delete_fb_product(product_id):
    if "business_id" not in session:
        return redirect("/login")

    ensure_fb_tables()
    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        DELETE FROM fb_products
        WHERE id = %s AND business_id = %s
        """,
        (product_id, business_id),
    )
    conn.commit()
    conn.close()

    return dashboard_redirect_with_toast("F&B product deleted.", "success", "fb")


@app.route("/fb/sales/close", methods=["POST"])
def close_fb_sale():
    if "business_id" not in session:
        return redirect("/login")

    ensure_fb_tables()
    business_id = session["business_id"]
    products = get_fb_products(business_id, active_only=True)

    items = []
    total_amount = 0.0

    for product in products:
        qty = safe_int(request.form.get(f"qty_{product['id']}") or 0, 0)
        if qty <= 0:
            continue

        unit_price = float(product.get("price") or 0)
        line_total = unit_price * qty
        total_amount += line_total
        items.append(
            {
                "product_id": product["id"],
                "product_name_snapshot": product["name"],
                "quantity": qty,
                "unit_price": unit_price,
                "line_total": line_total,
            }
        )

    if not items:
        return dashboard_redirect_with_toast("Choose at least one F&B item before closing the sale.", "error", "fb")

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO fb_sales (business_id, total_amount)
        VALUES (%s, %s)
        RETURNING id
        """,
        (business_id, total_amount),
    )
    sale_row = c.fetchone()
    sale_id = sale_row["id"] if isinstance(sale_row, dict) else sale_row[0]

    for item in items:
        c.execute(
            """
            INSERT INTO fb_sale_items (
                sale_id,
                product_id,
                product_name_snapshot,
                quantity,
                unit_price,
                line_total
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                sale_id,
                item["product_id"],
                item["product_name_snapshot"],
                item["quantity"],
                item["unit_price"],
                item["line_total"],
            ),
        )

    conn.commit()
    conn.close()

    return dashboard_redirect_with_toast(
        f"F&B sale closed successfully (${total_amount:.2f}).",
        "success",
        "fb",
    )


def is_reservation_extension_possible(business, reservation, increment_minutes=30):
    business_id = business["id"]
    date_iso = reservation["date"]
    start_time = normalize_time_str(reservation["time"]) or reservation["time"]
    resource_id = reservation.get("resource_id")
    service_name = reservation["service"]
    increment_minutes = int(increment_minutes or 0)
    base_duration = get_reservation_base_duration_minutes(business_id, service_name)
    extra_minutes = int(reservation.get("extra_minutes") or 0)
    proposed_total_duration = base_duration + extra_minutes + increment_minutes
    start_minutes = time_to_minutes(start_time)
    proposed_end_minutes = start_minutes + proposed_total_duration
    shared_pool = get_service_shared_pool_key(business_id, service_name)
    if resource_id:
        rules = get_resource_day_rules(business_id, resource_id, date_iso)
        resource = get_resource_by_id(resource_id, business_id)
    else:
        rules = get_day_rules(business_id, date_iso)
        resource = None
    if rules.get("closed"):
        return False, "Resource is unavailable on that date."
    if proposed_end_minutes > time_to_minutes(rules["close_time"]):
        return False, "The next 30 minutes are outside working hours."
    capacity = get_service_pool_capacity(business_id, resource, service_name)
    current_units = get_service_capacity_units(business_id, service_name)
    rows = get_confirmed_reservations_for_date_excluding_fast(business_id, date_iso, excluded_reservation_id=reservation["id"])
    overlapping_units = 0
    for row in rows:
        existing_service = row["service"]
        existing_pool = get_service_shared_pool_key(business_id, existing_service)
        same_pool = existing_pool == shared_pool if shared_pool else (row.get("resource_id") == resource_id if resource_id else True)
        if not same_pool:
            continue
        existing_time = normalize_time_str(row["time"])
        if not existing_time:
            continue
        existing_start = time_to_minutes(existing_time)
        existing_duration = get_reservation_total_duration_minutes(business_id, existing_service, extra_minutes=row.get("extra_minutes") or 0)
        if ranges_overlap(start_minutes, proposed_total_duration, existing_start, existing_duration):
            overlapping_units += get_service_capacity_units(business_id, existing_service)
    if overlapping_units + current_units > capacity:
        return False, "The next 30 minutes are already booked."
    return True, None

@app.route("/reservations/add-30/<int:reservation_id>", methods=["POST"])
def add_30_minutes_to_reservation(reservation_id):
    if "business_id" not in session:
        return redirect("/login")

    ensure_business_feature_columns()
    ensure_reservation_extension_columns()

    business_id = session["business_id"]
    business = get_business_by_id(business_id)
    if not business:
        return dashboard_redirect_with_toast("Business not found.", "error", "reservations")

    feature_flags = get_business_feature_flags(business)
    if not feature_flags.get("enable_time_extension"):
        return dashboard_redirect_with_toast("Time extension is disabled for this business.", "error", "reservations")

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, customer_name, customer_phone, service, date, time, status,
               resource_id, resource_name_snapshot, google_event_id,
               COALESCE(extra_minutes, 0) AS extra_minutes,
               COALESCE(extra_price, 0) AS extra_price
        FROM reservations
        WHERE id = %s AND business_id = %s
        LIMIT 1
        """,
        (reservation_id, business_id),
    )
    reservation = c.fetchone()
    conn.close()

    if not reservation:
        return dashboard_redirect_with_toast("Reservation not found.", "error", "reservations")

    if (reservation.get("status") or "").upper() != "CONFIRMED":
        return dashboard_redirect_with_toast("Only confirmed reservations can be extended.", "error", "reservations")

    can_extend, error_message = is_reservation_extension_possible(business, reservation, increment_minutes=30)
    if not can_extend:
        return dashboard_redirect_with_toast(error_message or "The next 30 minutes are not available.", "error", "reservations")

    extra_charge = calculate_extension_extra_charge(
        business,
        reservation["service"],
        current_extra_minutes=reservation.get("extra_minutes") or 0,
        increment_minutes=30,
    )
    if extra_charge is None:
        return dashboard_redirect_with_toast(
            "No pricing rule was found for the extra 30 minutes. Add a longer duration service or set a flat 30-minute price.",
            "error",
            "reservations",
        )

    new_extra_minutes = int(reservation.get("extra_minutes") or 0) + 30
    new_extra_price = round(float(reservation.get("extra_price") or 0) + float(extra_charge), 2)

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        UPDATE reservations
        SET extra_minutes = %s,
            extra_price = %s
        WHERE id = %s AND business_id = %s
        """,
        (new_extra_minutes, new_extra_price, reservation_id, business_id),
    )
    conn.commit()
    conn.close()

    old_event_id = reservation.get("google_event_id")
    if old_event_id:
        try:
            delete_event(old_event_id, calendar_id=(business.get("calendar_id") or "primary"))
        except Exception as e:
            print("add_30_minutes_to_reservation delete_event warning:", e, flush=True)

    new_event = add_reservation_to_google_calendar(
        business_id,
        reservation["customer_name"],
        reservation["service"],
        reservation["date"],
        reservation["time"],
        resource_name=reservation.get("resource_name_snapshot"),
        resource_id=reservation.get("resource_id"),
        extra_minutes=new_extra_minutes,
    )
    if new_event and new_event.get("id"):
        save_google_event_id(reservation_id, new_event["id"])

    total_price = get_reservation_total_price(
        business_id,
        reservation["service"],
        extra_price=new_extra_price,
    )

    return dashboard_redirect_with_toast(
        f"Added 30 minutes successfully (+${extra_charge:.2f}). New total: ${total_price:.2f}.",
        "success",
        "reservations",
    )


# ------------------ RUN ------------------

if __name__ == "__main__":
    init_db()       # <-- creates tables automatically
    ensure_fb_tables()
    ensure_business_feature_columns()
    ensure_reservation_extension_columns()
    ensure_resource_availability_tables()
    app.run(host="0.0.0.0", port=10000)



