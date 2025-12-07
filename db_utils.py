# db_utils.py
import os
import psycopg2
import psycopg2.extras

# Render will use the DATABASE_URL environment variable
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL not set. Add it in Render Environment Variables")

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


def init_db():
    conn = get_db_connection()
    c = conn.cursor()

    # NO MORE DROPPING TABLES HERE IN PROD

    c.execute("""
        CREATE TABLE IF NOT EXISTS businesses (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            provider VARCHAR(50) DEFAULT 'Meta',
            phone_number_id VARCHAR(255) UNIQUE,
            access_token TEXT,
            calendar_id VARCHAR(255),
            timezone VARCHAR(64) DEFAULT 'Asia/Beirut',
            gcal_credentials TEXT
        );
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE
        );
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS services (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            price NUMERIC,
            duration_min INTEGER,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE
        );
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS reservations (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            customer_name VARCHAR(255),
            customer_phone VARCHAR(50),
            service VARCHAR(255),
            date VARCHAR(50),
            time VARCHAR(50),
            status VARCHAR(25)
        );
    """)

    conn.commit()
    conn.close()
    print("PostgreSQL schema initialized successfully.")




