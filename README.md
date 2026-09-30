# Amazon Reports

Sales reporting for multiple Amazon brands (one seller account per brand), across the US and Canadian stores. Built with Django.

- **Two kinds of users**
  - **Employees** see every brand, plus an all-brands overview. They manage brands, products and users.
  - **Clients** see one brand, read-only.
- **Dashboards** for all brands or a single brand. Each can show all stores or just US or CA.
- **Weekly and monthly reports** compare against the previous period and the same period last year.
- **Frozen share links**: "Share frozen link" on a report creates a URL whose numbers stay fixed until someone presses **Refresh data**. It's the Monday report you can send around.
- **Products are keyed by ASIN.** They're created automatically from order data, and you can edit the names used in reports.
- **Amazon data is append-only.** Every downloaded report is kept, and a new version of an order is stored only when it changes. Nothing is deleted, so reports can always be rebuilt from the raw data.

## Quick start (development)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"

export DJANGO_DEBUG=1
python manage.py migrate
python manage.py seed_demo      # 3 demo brands, fake order history, demo users
python manage.py runserver
```

Log in as `admin`/`admin` or `employee`/`employee` (all brands), or `client`/`client` (Northwind only).

Run the tests with `pytest`.

## How the data works

| Table | What it holds |
|---|---|
| `RawReport` | Every report file, stored gzipped under `DATA_DIR/media/raw_reports/`, with when it was fetched |
| `OrderVersion` | One snapshot of one order. A new one is stored only when the order's content hash changes; `is_current` marks the newest |
| `OrderLine` | The line items of a snapshot, with the full original row in `raw` |

- **Report numbers** come from `apps/sales/metrics.py`.
  - Revenue is the sum of `item-price` for lines that have a price. Pending orders count once Amazon prices them.
  - When US and CA are combined, CAD is converted to USD using the rate on each marketplace (editable in Django admin → Marketplaces).
- **Shared links** store a `data_cutoff` and query only the versions fetched before it, so late changes don't alter a report that's already been sent.
- **Dates** (days, weeks starting Monday, months) use `REPORT_TIME_ZONE` (default `America/Los_Angeles`).

## Getting data in

- **Dummy Amazon client** (default, `AMAZON_CLIENT=dummy`): generates realistic, stable fake orders so everything can be exercised before SP-API access is set up.
- **Amazon SP-API** (`AMAZON_CLIENT=sp_api`):
  1. Set `SP_API_LWA_APP_ID` and `SP_API_LWA_CLIENT_SECRET` for your app.
  2. Paste each brand's refresh token and selling partner ID under **Brands → Settings**. Tokens are stored encrypted.
  3. The sync requests `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` for the brand's marketplaces.
  - This path hasn't been tested against Amazon yet.
- **Manual upload**: Brands → Settings → Upload a report. This takes the same "All Orders" flat file you export from Seller Central.

Commands:

```bash
python manage.py sync_amazon                     # one sync of every enabled brand
python manage.py sync_amazon --every 3600        # keep syncing hourly (what the worker runs)
python manage.py sync_amazon --backfill-days 400 # load history by order date
python manage.py import_report <brand-slug> <files or folders> [--limit 1]
python manage.py import_legacy ~/.local/share/aws-reporting   # from the old Flask app
python manage.py compare_legacy ~/.local/share/aws-reporting  # check totals match the old app
```

## Testing with real Seller Central exports

The manual "All Orders" exports use the same format the SP-API sync downloads, so the saved exports are a good way to test the pipeline with real data.

**Option 1: everything from the old app at once**

```bash
python manage.py import_legacy ~/.local/share/aws-reporting    # brands, product names, all archived files
python manage.py compare_legacy ~/.local/share/aws-reporting --details
```

- `compare_legacy` compares monthly orders, units and sales per sales channel with the old app's `orders.sqlite`.
- It groups by UTC month, like the old app did, so the numbers should match exactly.
- With `--details`, a month that doesn't match lists the order IDs that only one side has.
- The command exits with an error if anything differs.

**Option 2: one file at a time, watching the app in between**

```bash
python manage.py import_report <brand-slug> path/to/raw/folder --limit 1
```

- Files are imported oldest first. Folders are searched recursively for `.txt/.tsv/.csv/.gz` files.
- Files already imported are skipped, so running the command again imports the next one.
- Each file prints how many orders were **new**, **changed** (e.g. Pending → Shipped) or **unchanged** since the previous file.
- Each file's date comes from the old app's archive timestamp in the file name if there is one. Otherwise it's the newest `last-updated-date` inside the file, and failing that, the file's modification time.

## Front-end

The styles use Tailwind CSS 4 and daisyUI 5, compiled into `static/css/app.css`, and Chart.js is copied to `static/vendor/`. Both are committed, so running the app doesn't need Node. After changing templates or classes, rebuild with:

```bash
npm install
npm run build        # or: npm run watch
```

## Deployment

```bash
cp .env.example .env          # set DJANGO_SECRET_KEY, FIELD_ENCRYPTION_KEY, hosts…
docker compose up -d --build  # Postgres + web (gunicorn) + hourly sync worker
docker compose exec web python manage.py createsuperuser
```

- **Data:** everything lives in the `pgdata` and `appdata` volumes, which hold the database and the raw report files.
- **HTTPS:** put the app behind HTTPS and set `DJANGO_HTTPS=1` and `DJANGO_CSRF_TRUSTED_ORIGINS`.
- **Access:** every page requires login.
- **User management:** use **Users** in the app. Superusers also get Django admin at `/admin/`.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `DJANGO_SECRET_KEY` | (required unless `DJANGO_DEBUG=1`) | |
| `DJANGO_DEBUG` | `0` | |
| `DJANGO_ALLOWED_HOSTS` | `localhost,127.0.0.1` | |
| `DATABASE_URL` | SQLite in `DATA_DIR` | e.g. `postgres://user:pass@host/db` |
| `DATA_DIR` | `./var` | SQLite DB and raw report files |
| `FIELD_ENCRYPTION_KEY` | derived from secret key | Fernet key for refresh tokens |
| `AMAZON_CLIENT` | `dummy` | `dummy` or `sp_api` |
| `REPORT_TIME_ZONE` | `America/Los_Angeles` | Day/week/month boundaries and displayed times |

## Legacy Flask app

`aws_reports/` is the previous Flask version, kept for comparison until the new app has been checked against it. `import_legacy` brings its brands, product names and archived report files into the new app. After that, the folder can be deleted.
