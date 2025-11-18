# Reservation_Bot.py — Flask + SQLite + WhatsApp + Google Calendar

import os
import sqlite3
import requests
from flask import Flask, request, render_template_string
from dotenv import load_dotenv
from gcal import create_event  # <-- requires gcal.py from earlier steps
from datetime import datetime, timedelta
from flask import request as flask_request  # at the top if not already

def get_business_by_phone_number_id(phone_number_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM businesses WHERE phone_number_id=?", (phone_number_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "id": row[0],
            "name": row[1],
            "phone_number_id": row[2],
            "access_token": row[3],
            "calendar_id": row[4],
            "timezone": row[5],
            "provider": row[6],
            "api_key": row[7]
        }
    return None
def get_business_by_id(business_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM businesses WHERE id=?", (business_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "id": row[0],
            "name": row[1],
            "phone_number_id": row[2],
            "access_token": row[3],
            "calendar_id": row[4],
            "timezone": row[5],
            "provider": row[6],
            "api_key": row[7]
        }
    return None



# ------------------ Flask ------------------
app = Flask(__name__)

# ------------------ Database (safe path) ------------------
DB_DIR  = r"C:\Users\Public\ReservationBotData"   # writable on Windows
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "my_database.db")

def init_db():
    # ensure file exists
    with open(DB_PATH, "ab"):
        pass

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1) Businesses table (supports meta + 360dialog)
    c.execute("""
        CREATE TABLE IF NOT EXISTS businesses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            phone_number_id TEXT,
            access_token TEXT,
            calendar_id TEXT,
            timezone TEXT,
            provider TEXT DEFAULT 'meta',
            api_key TEXT
        )
    """)

    # Safe ALTERs in case the table already existed
    try:
        c.execute("ALTER TABLE businesses ADD COLUMN provider TEXT DEFAULT 'meta'")
    except sqlite3.OperationalError:
        pass

    try:
        c.execute("ALTER TABLE businesses ADD COLUMN api_key TEXT")
    except sqlite3.OperationalError:
        pass

    # Ensure default business with id = 1
    c.execute("SELECT id FROM businesses WHERE id = 1")
    if not c.fetchone():
        c.execute(
            "INSERT INTO businesses (id, name, phone_number_id, access_token, calendar_id, timezone, provider) "
            "VALUES (1, ?, ?, ?, ?, ?, 'meta')",
            (
                BUSINESS_NAME,
                PHONE_NUMBER_ID or "",
                ACCESS_TOKEN or "",
                "primary",
                "Asia/Beirut"
            )
        )

    # 2) Reservations table (add business_id with default 1)
    c.execute("""
        CREATE TABLE IF NOT EXISTS reservations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone TEXT,
            name  TEXT,
            service TEXT,
            date  TEXT,
            time  TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # add business_id column if it doesn't exist
    try:
        c.execute("ALTER TABLE reservations ADD COLUMN business_id INTEGER DEFAULT 1")
    except sqlite3.OperationalError:
        # column already exists, ignore
        pass

    # 3) Services table (add business_id with default 1)
    c.execute("""
        CREATE TABLE IF NOT EXISTS services (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            price REAL NOT NULL,
            duration_min INTEGER NOT NULL DEFAULT 45
        )
    """)

    try:
        c.execute("ALTER TABLE services ADD COLUMN business_id INTEGER DEFAULT 1")
    except sqlite3.OperationalError:
        pass

    # seed services if empty
    c.execute("SELECT COUNT(*) FROM services")
    if c.fetchone()[0] == 0:
        c.executemany(
            "INSERT INTO services(name, price, duration_min, business_id) VALUES (?, ?, ?, 1)",
            [
                ("haircut", 15.0, 30),
                ("beard trim", 8.0, 20),
                ("hair + beard", 20.0, 45),
                ("consultation", 0.0, 30),
            ]
        )

    conn.commit()
    conn.close()
    print("DB ready (multi-business base schema)")


def get_db_connection():
    return sqlite3.connect(DB_PATH)

# ------------------ Config (.env) ------------------
load_dotenv()
ACCESS_TOKEN    = os.getenv("ACCESS_TOKEN")
VERIFY_TOKEN    = os.getenv("VERIFY_TOKEN", "khoury123")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
BUSINESS_NAME   = os.getenv("BUSINESS_NAME", "Demo Business")  # multibusiness


# ------------------ Helpers ------------------
user_state = {}  # phone -> step data

def send_message(to, text, business):
    """
    Send a WhatsApp message via either Meta Cloud API or 360dialog,
    depending on business['provider'].
    """
    provider = (business.get("provider") or "meta").lower()

    if provider == "360dialog":
        # 360dialog API
        url = "https://waba-v1.360dialog.io/messages"
        headers = {
            "D360-API-KEY": business.get("api_key", ""),
            "Content-Type": "application/json"
        }
        payload = {
            "recipient_type": "individual",
            "to": to,
            "type": "text",
            "text": {"body": text}
        }
    else:
        # Default: Meta Cloud API
        url = f"https://graph.facebook.com/v21.0/{business['phone_number_id']}/messages"
        headers = {
            "Authorization": f"Bearer {business['access_token']}",
            "Content-Type": "application/json"
        }
        payload = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": text}
        }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        print("send_message:", provider, r.status_code, r.text)
    except Exception as e:
        print("send_message error:", provider, e)

def process_incoming_message(business, phone, text):
    """
    Shared conversation logic for both Meta and 360dialog webhooks.
    """
    global user_state

    t = text.strip()
    lt = t.lower()

    # user_state key includes business_id so same phone can talk to multiple businesses
    key = (business["id"], phone)
    state = user_state.get(key)

    # booking intent
    if lt.startswith("book") or "book" in lt or "appointment" in lt or "reserve" in lt:
        user_state[key] = {"step": "awaiting_name"}
        send_message(phone, "Sure — what is your full name?", business)
        return "ok", 200

    # cancel intent
    if "cancel" in lt or "delete" in lt:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("DELETE FROM reservations WHERE business_id=? AND phone=?", (business["id"], phone))
        conn.commit()
        conn.close()
        user_state.pop(key, None)
        send_message(phone, "✅ All reservations under your number have been cancelled.", business)
        return "ok", 200

    # multi-step flow
    if state and state.get("step") == "awaiting_name":
        state["name"] = t
        state["step"] = "awaiting_service"
        send_message(phone, f"Thanks, {t}. Which service would you like? (e.g., haircut, consultation)", business)
        return "ok", 200

    if state and state.get("step") == "awaiting_service":
        state["service"] = t
        state["step"] = "awaiting_date"
        send_message(phone, "What date would you like? (e.g., 2025-11-20 or 20 Nov)", business)
        return "ok", 200

    if state and state.get("step") == "awaiting_date":
        state["date"] = t
        state["step"] = "awaiting_time"
        send_message(phone, "What time? (e.g., 16:00 or 4 PM)", business)
        return "ok", 200

    if state and state.get("step") == "awaiting_time":
        state["time"] = t

        # save reservation
        save_reservation(
            business["id"],
            phone,
            state.get("name", ""),
            state.get("service", ""),
            state.get("date", ""),
            state.get("time", "")
        )

        # optional: Google Calendar
        try:
            summary = f"{state.get('service')} – {state.get('name')}"
            description = f"From: {phone}\nService: {state.get('service')}\nWhen: {state.get('date')} {state.get('time')}"
            create_event(summary, state.get("date", ""), state.get("time", ""),
                         description=description,
                         calendar_id=business["calendar_id"],
                         duration_min=45)
            send_message(phone, "📅 Also added to our Google Calendar.", business)
        except Exception as e:
            print("gcal error:", e)

        send_message(
            phone,
            f"✅ Reservation confirmed for {state.get('service')} on {state.get('date')} at {state.get('time')}. "
            f"Thank you, {state.get('name')}!",
            business
        )
        user_state.pop(key, None)
        return "ok", 200

    # fallback
    send_message(
        phone,
        "Sorry, I didn't understand. Type 'book' to create a reservation or 'cancel' to cancel.",
        business
    )
    return "ok", 200


def save_reservation (business_id, phone, name, service, date, time_):
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO reservations (business_id, phone, name, service, date, time) VALUES (?, ?, ?, ?, ?, ?)",
            (business_id, phone, name, service, date, time_)
        )
        conn.commit()
        conn.close()
        print("SAVED:", phone, name, service, date, time_)
from datetime import datetime, timedelta

def get_service_info(business_id, service_name):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT price, duration_min FROM services WHERE business_id=? AND lower(name)=lower(?)",
        (business_id, service_name)
    )
    row = c.fetchone()
    conn.close()
    if row:
        return {"price": float(row[0]), "duration": int(row[1])}
    return {"price": 0.0, "duration": 45}



def normalize_time_str(tstr):
    # clean basic stuff
    tstr = tstr.strip().upper().replace(".", "")

    # pick the first token that has a digit (e.g. from "16:30 PLEASE")
    candidate = None
    for part in tstr.split():
        if any(ch.isdigit() for ch in part):
            candidate = part
            break
    if candidate is None:
        return None
    tstr = candidate

    # Insert space before AM/PM if missing (4PM -> 4 PM)
    if tstr.endswith("AM") or tstr.endswith("PM"):
        if len(tstr) > 2 and tstr[-3] != " ":
            tstr = tstr[:-2] + " " + tstr[-2:]

    for fmt in ["%H:%M", "%I %p", "%I:%M %p", "%H"]:
        try:
            t = datetime.strptime(tstr, fmt).time()
            return f"{t.hour:02d}:{t.minute:02d}"
        except:
            continue
    return None



def is_taken(date_str, time_str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT 1 FROM reservations WHERE date=? AND time=? LIMIT 1",
              (date_str, time_str))
    hit = c.fetchone() is not None
    conn.close()
    return hit


def suggest_slots(
    date_str,
    requested_time_str,
    open_start="09:00",
    open_end="18:00",
    step_min=30,
    max_suggestions=3
):
    norm_req = normalize_time_str(requested_time_str)
    if norm_req is None:
        norm_req = open_start

    req_time = datetime.strptime(norm_req, "%H:%M").time()
    start = datetime.strptime(open_start, "%H:%M").time()
    end   = datetime.strptime(open_end, "%H:%M").time()

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

    def minutes(t): return t.hour * 60 + t.minute
    target = minutes(req_time)
    free.sort(key=lambda t: abs(minutes(t) - target))

    return [f"{t.hour:02d}:{t.minute:02d}" for t in free[:max_suggestions]]


# ------------------ Routes ------------------
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # Verification (GET) – this is used by Meta when you first set up the webhook
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if mode == "subscribe" and token == VERIFY_TOKEN:
            print("WEBHOOK VERIFIED (Meta)")
            return challenge, 200
        return "Forbidden", 403

    # Handle incoming messages (POST)
    data = request.get_json(silent=True)
    print("INCOMING META:", data)

    try:
        # pick the business based on phone_number_id from Meta payload
        phone_number_id = data['entry'][0]['changes'][0]['value']['metadata']['phone_number_id']
        business = get_business_by_phone_number_id(phone_number_id)
        if not business:
            print("Unknown business:", phone_number_id)
            return "ok", 200

        # extract sender + text
        message = data['entry'][0]['changes'][0]['value']['messages'][0]
        phone = message['from']
        text = message.get('text', {}).get('body', '')
    except Exception as e:
        print("Meta webhook parse error:", e)
        return "ok", 200

    # Hand off to the shared conversation logic
    return process_incoming_message(business, phone, text)

@app.route("/webhook_360/<int:business_id>", methods=["POST"])
def webhook_360(business_id):
    business = get_business_by_id(business_id)
    if not business or (business.get("provider") or "").lower() != "360dialog":
        print("Unknown or non-360 business:", business_id)
        return "ok", 200

    data = request.get_json(silent=True)
    print("INCOMING 360:", data)

    try:
        # 360dialog typical payload: { "messages": [ { "from": "...", "text": { "body": "..." } } ] }
        message = data["messages"][0]
        phone = message["from"]
        text = message.get("text", {}).get("body", "")
    except Exception as e:
        print("360 webhook parse error:", e)
        return "ok", 200

    return process_incoming_message(business, phone, text)


@app.route("/reservations") # dashboard for all the services and the reservations
def reservations_page():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT id, phone, name, service, date, time, created_at FROM reservations ORDER BY id DESC')
    rows = c.fetchall()
    conn.close()
    html = """
    <h2>Reservations</h2>
    <table border="1" cellpadding="6">
    <tr><th>ID</th><th>Phone</th><th>Name</th><th>Service</th><th>Date</th><th>Time</th><th>Created</th></tr>
    {% for r in rows %}
      <tr>{% for v in r %}<td>{{v}}</td>{% endfor %}</tr>
    {% endfor %}
    </table>
    """
    return render_template_string(html, rows=rows)
@app.route("/dashboard/<int:business_id>")
def dashboard(business_id):
    conn = get_db_connection()
    c = conn.cursor()

    # Fetch business info
    c.execute("SELECT name FROM businesses WHERE id=?", (business_id,))
    b = c.fetchone()
    if not b:
        return f"No business found with ID {business_id}", 404

    business_name = b[0]

    # Fetch services for this business
    c.execute("SELECT id, name, price, duration_min FROM services WHERE business_id=? ORDER BY id", (business_id,))
    services = c.fetchall()

    # Fetch recent reservations for this business
    c.execute("""
        SELECT id, phone, name, service, date, time, created_at
        FROM reservations
        WHERE business_id=?
        ORDER BY created_at DESC
        LIMIT 20
    """, (business_id,))
    reservations = c.fetchall()

    conn.close()

    html = """
    <h1>Dashboard — {{ business_name }}</h1>

    <h2>Services</h2>
    <table border="1" cellpadding="6">
      <tr><th>ID</th><th>Name</th><th>Price</th><th>Duration (min)</th></tr>
      {% for s in services %}
        <tr><td>{{s[0]}}</td><td>{{s[1]}}</td><td>{{s[2]}}</td><td>{{s[3]}}</td></tr>
      {% endfor %}
    </table>

    <h2 style="margin-top:30px;">Recent Reservations</h2>
    <table border="1" cellpadding="6">
      <tr>
        <th>ID</th><th>Phone</th><th>Name</th><th>Service</th>
        <th>Date</th><th>Time</th><th>Created</th>
      </tr>
      {% for r in reservations %}
        <tr>
          <td>{{r[0]}}</td><td>{{r[1]}}</td><td>{{r[2]}}</td><td>{{r[3]}}</td>
          <td>{{r[4]}}</td><td>{{r[5]}}</td><td>{{r[6]}}</td>
        </tr>
      {% endfor %}
    </table>

    """

    return render_template_string(
        html,
        business_name=business_name,
        services=services,
        reservations=reservations
    )

@app.route("/admin/businesses", methods=["GET", "POST"])
def admin_businesses():
    conn = get_db_connection()
    c = conn.cursor()

    if flask_request.method == "POST":
        name            = flask_request.form.get("name", "").strip()
        provider        = flask_request.form.get("provider", "meta").strip().lower()
        phone_number_id = flask_request.form.get("phone_number_id", "").strip()
        access_token    = flask_request.form.get("access_token", "").strip()
        api_key         = flask_request.form.get("api_key", "").strip()
        calendar_id     = flask_request.form.get("calendar_id", "primary").strip()
        timezone        = flask_request.form.get("timezone", "Asia/Beirut").strip()

        if name:
            if provider == "360dialog":
                # For 360dialog, only api_key is required (no Meta token / phone_number_id)
                if api_key:
                    c.execute(
                        "INSERT INTO businesses(name, phone_number_id, access_token, calendar_id, timezone, provider, api_key) "
                        "VALUES (?, '', '', ?, ?, '360dialog', ?)",
                        (name, calendar_id, timezone, api_key)
                    )
                    conn.commit()
            else:
                # Default: Meta Cloud API
                if phone_number_id and access_token:
                    c.execute(
                        "INSERT INTO businesses(name, phone_number_id, access_token, calendar_id, timezone, provider, api_key) "
                        "VALUES (?, ?, ?, ?, ?, 'meta', NULL)",
                        (name, phone_number_id, access_token, calendar_id, timezone)
                    )
                    conn.commit()

    # List all businesses
    c.execute("SELECT id, name, phone_number_id, provider, timezone FROM businesses ORDER BY id")
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
        <td><a href="/dashboard/{{b[0]}}">Dashboard</a></td>
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


@app.route("/admin/<int:business_id>/services", methods=["GET", "POST"])
def admin_services(business_id):
    conn = get_db_connection()
    c = conn.cursor()

    # check business
    c.execute("SELECT name FROM businesses WHERE id=?", (business_id,))
    b = c.fetchone()
    if not b:
        conn.close()
        return f"No business with ID {business_id}", 404
    business_name = b[0]

    if flask_request.method == "POST":
        name   = flask_request.form.get("name", "").strip()
        price  = flask_request.form.get("price", "").strip()
        dur    = flask_request.form.get("duration_min", "").strip()

        try:
            price_f = float(price)
        except:
            price_f = 0.0
        try:
            dur_i = int(dur)
        except:
            dur_i = 30

        if name:
            c.execute(
                "INSERT INTO services(name, price, duration_min, business_id) VALUES (?, ?, ?, ?)",
                (name, price_f, dur_i, business_id)
            )
            conn.commit()

    # list services for this business
    c.execute("SELECT id, name, price, duration_min FROM services WHERE business_id=? ORDER BY id", (business_id,))
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
      <a href="/dashboard/{{business_id}}">Back to dashboard</a> |
      <a href="/admin/businesses">Back to businesses</a>
    </p>
    """
    return render_template_string(html, business_name=business_name, business_id=business_id, rows=rows)

# ------------------ Run ------------------
if __name__ == "__main__":
    print("Using DB:", DB_PATH)
    init_db()

    # On Render, PORT is provided as an env var. Locally defaults to 5000.
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)


