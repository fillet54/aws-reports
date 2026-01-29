import sqlite3
from typing import Optional, Dict

from werkzeug.security import check_password_hash, generate_password_hash

from .config import USER_DB_PATH


def get_user_db():
    conn = sqlite3.connect(USER_DB_PATH)
    conn.row_factory = sqlite3.Row
    init_user_db(conn)
    return conn


def init_user_db(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL
        );
        """
    )
    conn.commit()


def _row_to_user(row) -> Optional[Dict]:
    if not row:
        return None
    return {
        "id": row["id"],
        "username": row["username"],
        "password_hash": row["password_hash"],
    }


def get_users() -> list[Dict]:
    conn = get_user_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users;")
        return [_row_to_user(user) for user in cur.fetchall()]
    finally:
        conn.close()

def get_user_by_id(user_id: int) -> Optional[Dict]:
    conn = get_user_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        return _row_to_user(cur.fetchone())
    finally:
        conn.close()


def get_user_by_username(username: str) -> Optional[Dict]:
    conn = get_user_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = ?", (username.strip(),))
        return _row_to_user(cur.fetchone())
    finally:
        conn.close()


def create_user(username: str, password: str) -> Dict:
    username = username.strip()
    if not username:
        raise ValueError("Username is required.")
    if not password:
        raise ValueError("Password is required.")

    conn = get_user_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (username, password_hash) VALUES (?, ?)",
            (username, generate_password_hash(password)),
        )
        conn.commit()
        user_id = cur.lastrowid
        return {"id": user_id, "username": username, "password_hash": None}
    finally:
        conn.close()

def update_user(username: str, new_password: str) -> Dict:
    username = username.strip()
    if not username:
        raise ValueError("Username is required.")
    if not new_password:
        raise ValueError("Password is required.")

    if get_user_by_username(username) is None:
        raise ValueError("Username is unknown")

    conn = get_user_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET password_hash = ? WHERE username = ?",
            (generate_password_hash(new_password), username),
        )
        conn.commit()
        user_id = cur.lastrowid
        return {"id": user_id, "username": username, "password_hash": None}
    finally:
        conn.close()


def verify_user(username: str, password: str) -> Optional[Dict]:
    user = get_user_by_username(username)
    if not user:
        return None
    if not check_password_hash(user["password_hash"], password):
        return None
    return user
