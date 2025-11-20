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
