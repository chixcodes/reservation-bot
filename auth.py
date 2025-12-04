from flask import Blueprint, render_template, request, redirect, session, url_for
from werkzeug.security import generate_password_hash, check_password_hash
import sqlite3

auth = Blueprint('auth', __name__)

def get_db():
    return sqlite3.connect("reservation.db")

# Signup route (for adding businesses manually for now)
@auth.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        business_name = request.form['business_name']
        username = request.form['username']
        password = request.form['password']

        conn = get_db()
        c = conn.cursor()

        # Create business entry
        c.execute("INSERT INTO businesses (name) VALUES (?)", (business_name,))
        business_id = c.lastrowid

        # Create user for business
        c.execute("INSERT INTO business_users (business_id, username, password_hash) VALUES (?, ?, ?)",
                  (business_id, username, generate_password_hash(password)))

        conn.commit()
        conn.close()

        return redirect('/login')

    return render_template('register.html')


# Login route
@auth.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT id, business_id, password_hash FROM business_users WHERE username=?", (username,))
        user = c.fetchone()
        conn.close()

        if user and check_password_hash(user[2], password):
            session['user_id'] = user[0]
            session['business_id'] = user[1]
            return redirect('/dashboard')
        else:
            return "Invalid credentials"

    return render_template('login.html')


@auth.route('/logout')
def logout():
    session.clear()
    return redirect('/login')
