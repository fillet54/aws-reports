# aws-reporting

## User management (create users / reset passwords)

User accounts are stored in a dedicated SQLite database (see “Default database location” below).

### Create a user

Run from the repo root (or anywhere the `aws_reports` package is importable):

```bash
python - <<'PY'
from aws_reports.user_db import create_user

create_user("alice", "change-me")
print("created user: alice")
PY
```

### Reset a user’s password

This updates the `password_hash` for an existing user in `users.sqlite`:

```bash
python - <<'PY'
import sqlite3
from werkzeug.security import generate_password_hash

from aws_reports.config import USER_DB_PATH

username = "alice"
new_password = "change-me-again"

conn = sqlite3.connect(USER_DB_PATH)
try:
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET password_hash = ? WHERE username = ?",
        (generate_password_hash(new_password), username),
    )
    if cur.rowcount == 0:
        raise SystemExit(f"no such user: {username!r}")
    conn.commit()
    print(f"password reset for: {username}")
finally:
    conn.close()
PY
```

## Default database location

All application data is stored under the per-user data directory `DATA_DIR`, which is computed by `aws_reports.userdirs.user_data_dir("aws-reporting")`.

### Quick: print the paths on this machine

```bash
python - <<'PY'
from aws_reports.config import DATA_DIR, USER_DB_PATH, BRANDS_FILE

print("DATA_DIR     =", DATA_DIR)
print("USER_DB_PATH =", USER_DB_PATH)
print("BRANDS_FILE  =", BRANDS_FILE)
PY
```

### What’s stored where

- Users DB: `DATA_DIR/users.sqlite`
- Brands index: `DATA_DIR/brands.json`
- Per-brand orders DB: `DATA_DIR/brands/<brand_id>/orders.sqlite`
- Upload temp dir: `DATA_DIR/tmp_uploads/`

### Defaults by OS

- Linux: `$XDG_DATA_HOME/aws-reporting` (or `~/.local/share/aws-reporting`)
- macOS: `~/Library/Application Support/aws-reporting`
- Windows: `%LOCALAPPDATA%\\aws-reporting` (or `%APPDATA%\\aws-reporting` fallback)

