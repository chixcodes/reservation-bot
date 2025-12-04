import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), "my_database.db")

def create_database():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reservations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT,
        name TEXT,
        service TEXT,
        date TEXT,
        time TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()
    print("✅ Database created successfully at", DB_PATH)

if __name__ == "__main__":
    create_database()
import sqlite3

def init_db():
    conn = sqlite3.connect("reservation.db")
    c = conn.cursor()

    # Businesses (each client salon/clinic/etc.)
    c.execute("""
        CREATE TABLE IF NOT EXISTS businesses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            whatsapp_number TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Business login accounts
    c.execute("""
        CREATE TABLE IF NOT EXISTS business_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            username TEXT UNIQUE,
            password_hash TEXT,
            role TEXT DEFAULT 'owner',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES businesses(id)
        )
    """)

    # Reservations
    c.execute("""
        CREATE TABLE IF NOT EXISTS reservations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            customer_name TEXT,
            customer_phone TEXT,
            service TEXT,
            date TEXT,
            time TEXT,
            status TEXT DEFAULT 'PENDING', -- PENDING/CONFIRMED/CANCELED
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES businesses(id)
        )
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    print("Database ready ✔")
