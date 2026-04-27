import os
import psycopg2
import psycopg2.extras

# Render will use the DATABASE_URL environment variable
DATABASE_URL = os.getenv("DATABASE_URL")


def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL not set. Add it in Render Environment Variables")

    conn = psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    return conn


def init_db():
    conn = get_db_connection()
    c = conn.cursor()

    # NO MORE DROPPING TABLES HERE IN PROD

    # --------------------------------------------------
    # CORE TABLES
    # --------------------------------------------------

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

    # KEEP THIS FOR NOW.
    # Your current booking flow still relies on single business-wide slot locking.
    # We will replace/drop this later when we switch the booking engine to resources.
    c.execute("""
        DROP INDEX IF EXISTS unique_confirmed_slot;
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS business_hours (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            weekday INTEGER NOT NULL CHECK (weekday BETWEEN 0 AND 6),
            is_closed BOOLEAN DEFAULT FALSE,
            open_time TIME,
            close_time TIME,
            UNIQUE (business_id, weekday)
        );
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS blocked_dates (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            blocked_date DATE NOT NULL,
            note VARCHAR(255),
            UNIQUE (business_id, blocked_date)
        );
    """)

    # --------------------------------------------------
    # EXISTING SAFE ALTERATIONS
    # --------------------------------------------------

    c.execute("""
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS role VARCHAR(20) DEFAULT 'business';
    """)

    c.execute("""
        ALTER TABLE reservations
        ADD COLUMN IF NOT EXISTS notes TEXT;
    """)

    c.execute("""
        ALTER TABLE reservations
        ADD COLUMN IF NOT EXISTS google_event_id VARCHAR(255);
    """)

    c.execute("""
        ALTER TABLE businesses
        ADD COLUMN IF NOT EXISTS preferred_language VARCHAR(20) DEFAULT 'auto';
    """)

    c.execute("""
        ALTER TABLE businesses
        ADD COLUMN IF NOT EXISTS assistant_tone VARCHAR(50) DEFAULT 'friendly';
    """)

    c.execute("""
        ALTER TABLE businesses
        ADD COLUMN IF NOT EXISTS custom_welcome_message TEXT;
    """)

    c.execute("""
        ALTER TABLE businesses
        ADD COLUMN IF NOT EXISTS business_description TEXT;
    """)

    # --------------------------------------------------
    # RESOURCE-BASED SCHEDULING (PHASE 1)
    # --------------------------------------------------
    # This is the foundation for:
    # - staff booking (barbers, doctors)
    # - court / room booking (padel, studios)
    # - shared pool capacity (group sessions, multi-seat)
    #
    # IMPORTANT:
    # These additions do NOT change live booking behavior yet.
    # They just prepare the DB for the next phase.

    c.execute("""
        ALTER TABLE businesses
        ADD COLUMN IF NOT EXISTS scheduling_mode VARCHAR(30) DEFAULT 'single';
    """)
    # Examples later:
    # single   -> current behavior
    # staff    -> named staff resources
    # resource -> courts / rooms
    # pool     -> capacity-based pooled booking

    c.execute("""
        ALTER TABLE businesses
        ADD COLUMN IF NOT EXISTS allow_customer_resource_choice BOOLEAN DEFAULT FALSE;
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS resources (
            id SERIAL PRIMARY KEY,
            business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            resource_type VARCHAR(30) NOT NULL DEFAULT 'staff',
            capacity INTEGER NOT NULL DEFAULT 1,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            display_order INTEGER NOT NULL DEFAULT 0,
            color_tag VARCHAR(30),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (business_id, name)
        );
    """)

    # resource_type examples:
    # - staff
    # - doctor
    # - court
    # - room
    # - pool

    c.execute("""
        CREATE TABLE IF NOT EXISTS resource_services (
            id SERIAL PRIMARY KEY,
            business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
            resource_id INTEGER NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            service_id INTEGER NOT NULL REFERENCES services(id) ON DELETE CASCADE,
            UNIQUE (resource_id, service_id)
        );
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS resource_hours (
            id SERIAL PRIMARY KEY,
            business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
            resource_id INTEGER NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            weekday INTEGER NOT NULL CHECK (weekday BETWEEN 0 AND 6),
            is_closed BOOLEAN DEFAULT FALSE,
            open_time TIME,
            close_time TIME,
            UNIQUE (resource_id, weekday)
        );
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS resource_blocked_dates (
            id SERIAL PRIMARY KEY,
            business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
            resource_id INTEGER NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            blocked_date DATE NOT NULL,
            note VARCHAR(255),
            UNIQUE (resource_id, blocked_date)
        );
    """)

    c.execute("""
        ALTER TABLE reservations
        ADD COLUMN IF NOT EXISTS resource_id INTEGER REFERENCES resources(id) ON DELETE SET NULL;
    """)

    c.execute("""
        ALTER TABLE reservations
        ADD COLUMN IF NOT EXISTS resource_name_snapshot VARCHAR(255);
    """)

    # --------------------------------------------------
    # INDEXES FOR PERFORMANCE
    # --------------------------------------------------

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_services_business_id
        ON services (business_id);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_reservations_business_id
        ON reservations (business_id);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_reservations_business_date
        ON reservations (business_id, date);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_reservations_business_status
        ON reservations (business_id, status);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_reservations_business_resource_date
        ON reservations (business_id, resource_id, date);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_resources_business_id
        ON resources (business_id);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_resources_business_type
        ON resources (business_id, resource_type);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_resource_services_business_id
        ON resource_services (business_id);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_resource_services_resource_id
        ON resource_services (resource_id);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_resource_hours_resource_id
        ON resource_hours (resource_id);
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_resource_blocked_dates_resource_id
        ON resource_blocked_dates (resource_id);
    """)

    conn.commit()
    conn.close()
    print("PostgreSQL schema initialized successfully.")