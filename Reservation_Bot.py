# Reservation_Bot.py — CLEANED VERSION

import os
import sqlite3
import requests
import json
from datetime import datetime, timedelta

from dotenv import load_dotenv
from flask import (
    Flask,
    request,
    render_template,
    render_template_string,
    session,
    redirect,
    url_for,
)
from werkzeug.security import generate_password_hash, check_password_hash

from gcal import create_event  # your existing gcal helper


# ------------------ DB CONFIG ------------------

DB_FILENAME = "reservation_v2.db"


def get_db_connection():
    conn = sqlite3.connect(DB_FILENAME)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    c = conn.cursor()

    # Businesses table (one row per client / WhatsApp number)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS businesses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone_number_id TEXT,
            access_token TEXT,
            calendar_id TEXT,
            timezone TEXT,
            provider TEXT,
            api_key TEXT
        )
        """
    )

    # Users table (for dashboard login)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            email TEXT UNIQUE,
            password_hash TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES businesses(id)
        )
        """
    )

    # Services table (per business)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS services (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            name TEXT,
            price REAL,
            duration_min INTEGER,
            FOREIGN KEY (business_id) REFERENCES businesses(id)
        )
        """
    )

    # Reservations table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS reservations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            customer_name TEXT,
            customer_phone TEXT,
            service TEXT,
            date TEXT,
            time TEXT,
            status TEXT DEFAULT 'PENDING', -- PENDING / CONFIRMED / CANCELED
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES businesses(id)
        )
        """
    )

    conn.commit()
    conn.close()


# ------------------ BUSINESS HELPERS ------------------


def get_business_by_phone_number_id(phone_number_id: str):
    conn = get_db_connection()
    c = conn.cursor()

    # Try to match by phone_number_id
    c.execute(
        "SELECT * FROM businesses WHERE phone_number_id=? LIMIT 1",
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
    c.execute("SELECT * FROM businesses WHERE id=?", (business_id,))
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
    """
    Send a WhatsApp message via Meta Cloud API.
    """
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
        "SELECT price, duration_min FROM services WHERE business_id=? AND lower(name)=lower(?)",
        (business_id, service_name),
    )
    row = c.fetchone()
    conn.close()
    if row:
        return {"price": float(row[0]), "duration": int(row[1])}
    return {"price": 0.0, "duration": 45}


def get_service_names_for_business(business_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT name FROM services WHERE business_id=?", (business_id,))
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]


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


def is_taken(date_str, time_str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT 1 FROM reservations WHERE date=? AND time=? LIMIT 1",
        (date_str, time_str),
    )
    hit = c.fetchone() is not None
    conn.close()
    return hit


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
        if not is_taken(date_str, hhmm):
            free.append(t)

    def minutes(t):
        return t.hour * 60 + t.minute

    target = minutes(req_time)
    free.sort(key=lambda t: abs(minutes(t) - target))

    return [f"{t.hour:02d}:{t.minute:02d}" for t in free[:max_suggestions]]


def save_reservation(business_id, phone, name, service, date, time_):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO reservations (business_id, customer_name, customer_phone, service, date, time, status)
        VALUES (?, ?, ?, ?, ?, ?, 'CONFIRMED')
        """,
        (business_id, name, phone, service, date, time_),
    )
    conn.commit()
    conn.close()
    print(f"SAVED (CONFIRMED) -> {name}, {service} on {date} at {time_}")



def send_reservation_confirmation(phone, name, service, date, time, business):
    message = (
        f"✅ Your reservation is confirmed!\n"
        f"Name: {name}\n"
        f"Service: {service}\n"
        f"Date: {date}\n"
        f"Time: {time}\n\n"
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


# ------------------ CONVERSATION LOGIC ------------------


def process_incoming_message(business, phone, text):
    global user_state

    t = text.strip()
    lt = t.lower()

    key = (business["id"], phone)
    state = user_state.get(key)

    # START BOOKING
    if (
        "book" in lt
        or "appointment" in lt
        or "reserve" in lt
        or lt.startswith("book")
    ):
        user_state[key] = {"step": "awaiting_name"}
        send_message(phone, "Sure — what is your full name?", business)
        return "ok", 200

    # CANCEL BOOKING
    if "cancel" in lt or "delete" in lt:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "DELETE FROM reservations WHERE business_id=? AND customer_phone=?",
            (business["id"], phone),
        )
        conn.commit()
        conn.close()
        user_state.pop(key, None)
        send_message(
            phone, "✅ All reservations under your number have been cancelled.", business
        )
        return "ok", 200

    # STEP 1 – NAME
    if state and state.get("step") == "awaiting_name":
        state["name"] = t
        state["step"] = "awaiting_service"
        send_message(
            phone,
            f"Thanks, {t}. Which service would you like? (e.g., haircut, consultation)",
            business,
        )
        return "ok", 200

    # STEP 2 – SERVICE (keywords + AI fallback)
    if state and state.get("step") == "awaiting_service":
        lt2 = t.lower()

        # Detect all matching canonical services
        matched = set()
        for kw, canonical in SERVICE_KEYWORDS.items():
            if kw.lower() in lt2:
                matched.add(canonical)

        ai_service = None
        normalized = None

        # Special case: user asked for both haircut & beard
        if "Haircut" in matched and "Beard Trim" in matched:
            normalized = "Haircut and Beard"
        elif matched:
            # If we matched at least one, just take one of them
            normalized = next(iter(matched))
        else:
            # No keyword match, try AI
            if OPENROUTER_API_KEY:
                ai_service = ai_pick_service(business, t)
                if ai_service:
                    normalized = ai_service

        # Fallback: use raw text if nothing worked
        if normalized is None:
            normalized = t

        print("SERVICE STEP raw:", t, "ai:", ai_service, "normalized:", normalized)

        state["service"] = normalized
        state["step"] = "awaiting_date"
        send_message(
            phone,
            f"Great — {normalized}. What date would you like? (e.g., 20 Nov or 2025-11-20)",
            business,
        )
        return "ok", 200


    # STEP 3 – DATE
    if state and state.get("step") == "awaiting_date":
        state["date"] = t
        state["step"] = "awaiting_time"
        send_message(
            phone, "Perfect — and what time? (e.g., 16:00 or 4 PM)", business
        )
        return "ok", 200

    # STEP 4 – TIME
    if state and state.get("step") == "awaiting_time":
        raw_time = t
        raw_date = state.get("date", "")

        norm_time = normalize_time_str(raw_time)
        if not norm_time:
            send_message(
                phone,
                "Sorry, I couldn't understand the time. Please send something like 16:00 or 4 PM.",
                business,
            )
            return "ok", 200

        if is_taken(raw_date, norm_time):
            suggestions = suggest_slots(raw_date, norm_time)
            if suggestions:
                msg = (
                    f"❌ Sorry, {norm_time} on {raw_date} is already taken.\n\n"
                    f"Available nearby times:\n"
                    + "\n".join(f"• {s}" for s in suggestions)
                    + "\n\nPlease choose one of these times."
                )
            else:
                msg = (
                    f"❌ Sorry, {norm_time} on {raw_date} is already taken, and I couldn't "
                    f"find other free slots. Please send another time or date."
                )
            send_message(phone, msg, business)
            return "ok", 200

        state["time"] = norm_time

        service_info = get_service_info(business["id"], state["service"])
        price = service_info["price"]
        duration = service_info["duration"]

        save_reservation(
            business["id"],
            phone,
            state.get("name", ""),
            state.get("service", ""),
            state.get("date", ""),
            state.get("time", ""),
        )

        confirmation_msg = (
            f"✅ Reservation confirmed!\n\n"
            f"📌 *Service:* {state.get('service')}\n"
            f"💵 *Price:* ${price:.2f}\n"
            f"⏱ *Duration:* {duration} minutes\n"
            f"📅 *Date:* {state.get('date')}\n"
            f"⏰ *Time:* {state.get('time')}\n\n"
            f"Thank you, {state.get('name')}!"
        )
        send_message(phone, confirmation_msg, business)

        # GOOGLE CALENDAR
        try:
            summary = f"{state.get('service')} – {state.get('name')}"
            description = (
                f"From: {phone}\n"
                f"Service: {state.get('service')}\n"
                f"Price: {price}\n"
                f"Duration: {duration} min\n"
                f"When: {state.get('date')} {state.get('time')}"
            )

            # Use business-specific calendar if set, otherwise default to 'primary'
            calendar_id = business.get("calendar_id") or "primary"

            create_event(
                summary,
                state.get("date", ""),
                state.get("time", ""),
                description=description,
                calendar_id=calendar_id,
                duration_min=duration,
            )

        except Exception as e:
            print("gcal error:", e)



    # FALLBACK: stay silent instead of sending an extra confusing message
    print("FALLBACK: message not handled:", text)
    return "ok", 200



# ------------------ WEBHOOK ------------------


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

    data = request.get_json(silent=True)
    print("INCOMING META:", data)

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

    business = get_business_by_phone_number_id(phone_number_id)
    if not business:
        print("No business configured for phone_number_id", phone_number_id)
        return "ok", 200

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


# ------------------ ADMIN BUSINESSES ------------------


from flask import request as flask_request


@app.route("/admin/businesses", methods=["GET", "POST"])
def admin_businesses():
    conn = get_db_connection()
    c = conn.cursor()

    if flask_request.method == "POST":
        name = flask_request.form.get("name", "").strip()
        provider = flask_request.form.get("provider", "").strip()
        phone_number_id = flask_request.form.get("phone_number_id", "").strip()
        access_token = flask_request.form.get("access_token", "").strip()
        api_key = flask_request.form.get("api_key", "").strip()
        calendar_id = flask_request.form.get("calendar_id", "primary").strip()
        timezone = flask_request.form.get("timezone", "Asia/Beirut").strip()

        if name and phone_number_id and access_token:
            c.execute(
                """
                INSERT INTO businesses(name, phone_number_id, access_token, calendar_id, timezone, provider, api_key)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (name, phone_number_id, access_token, calendar_id, timezone, provider, api_key),
            )
            conn.commit()

    c.execute(
        "SELECT id, name, phone_number_id, provider, timezone FROM businesses ORDER BY id"
    )
    rows = c.fetchall()
    conn.close()

    html = """
    <h1>Admin — Businesses</h1>

    <h2>Existing businesses</h2>
    <table border="1" cellpadding="6">
      <tr>
        <th>ID</th>
        <th>Name</th>
        <th>Provider</th>
        <th>Phone Number ID</th>
        <th>Timezone</th>
        <th>Dashboard</th>
        <th>Services</th>
      </tr>
      {% for b in rows %}
      <tr>
        <td>{{b[0]}}</td>
        <td>{{b[1]}}</td>
        <td>{{b[3]}}</td>
        <td>{{b[2]}}</td>
        <td>{{b[4]}}</td>
        <td><a href="/dashboard">Dashboard</a></td>
        <td><a href="/admin/{{b[0]}}/services">Manage services</a></td>
      </tr>
      {% endfor %}
    </table>

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

      <p>360dialog API Key (if provider is 360dialog): <input name="api_key" style="width:400px;"></p>

      <p>Calendar ID: <input name="calendar_id" value="primary"></p>
      <p>Timezone: <input name="timezone" value="Asia/Beirut"></p>

      <p><button type="submit">Add business</button></p>
    </form>
    """

    return render_template_string(html, rows=rows)


# ------------------ ADMIN SERVICES ------------------


@app.route("/admin/<int:business_id>/services", methods=["GET", "POST"])
def admin_services(business_id):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("SELECT name FROM businesses WHERE id=?", (business_id,))
    b = c.fetchone()
    if not b:
        conn.close()
        return f"No business with ID {business_id}", 404
    business_name = b[0]

    if flask_request.method == "POST":
        name = flask_request.form.get("name", "").strip()
        price = flask_request.form.get("price", "").strip()
        dur = flask_request.form.get("duration_min", "").strip()

        try:
            price_f = float(price)
        except Exception:
            price_f = 0.0
        try:
            dur_i = int(dur)
        except Exception:
            dur_i = 30

        if name:
            c.execute(
                "INSERT INTO services(name, price, duration_min, business_id) VALUES (?, ?, ?, ?)",
                (name, price_f, dur_i, business_id),
            )
            conn.commit()

    c.execute(
        "SELECT id, name, price, duration_min FROM services WHERE business_id=? ORDER BY id",
        (business_id,),
    )
    rows = c.fetchall()
    conn.close()

    html = """
    <h1>Services — {{ business_name }}</h1>

    <table border="1" cellpadding="6">
      <tr><th>ID</th><th>Name</th><th>Price</th><th>Duration (min)</th></tr>
      {% for s in rows %}
      <tr>
        <td>{{s[0]}}</td>
        <td>{{s[1]}}</td>
        <td>{{s[2]}}</td>
        <td>{{s[3]}}</td>
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
      <a href="/dashboard">Back to dashboard</a> |
      <a href="/admin/businesses">Back to businesses</a>
    </p>
    """
    return render_template_string(
        html, business_name=business_name, business_id=business_id, rows=rows
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
                "INSERT INTO users(business_id, email, password_hash) VALUES (?, ?, ?)",
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
            "SELECT id, business_id, password_hash FROM users WHERE email=?", (email,)
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
    if "business_id" not in session:
        return redirect("/login")

    business_id = session["business_id"]

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, customer_name, service, date, time, status
        FROM reservations
        WHERE business_id = ?
        ORDER BY date, time
        """,
        (business_id,),
    )
    reservations = c.fetchall()
    conn.close()

    return render_template("dashboard.html", reservations=reservations)


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
        WHERE id = ? AND business_id = ?
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
        WHERE id = ? AND business_id = ?
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
        SELECT customer_phone, customer_name, service, date, time
        FROM reservations
        WHERE id = ? AND business_id = ?
        """,
        (reservation_id, business_id),
    )
    row = c.fetchone()

    if not row:
        conn.close()
        return redirect("/dashboard")

    phone, name, service, date, time = row

    c.execute(
        """
        UPDATE reservations
        SET status = 'CANCELED'
        WHERE id = ? AND business_id = ?
        """,
        (reservation_id, business_id),
    )
    conn.commit()
    conn.close()

    business = get_business_by_id(business_id)
    if business:
        try:
            send_reservation_cancellation(phone, name, service, date, time, business)
        except Exception as e:
            print("Error sending WhatsApp cancellation:", e)

    return redirect("/dashboard")


# ------------------ RUN ------------------

if __name__ == "__main__":
    print("Using DB file:", DB_FILENAME)
    init_db()

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)



