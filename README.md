# Amazon Reports

Sales reporting for multiple Amazon brands (one seller account per brand), across the US and Canadian stores. Built with Django.

- **Two kinds of users**
  - **Employees** see every brand, plus an all-brands overview. They manage brands, products and users.
  - **Clients** see one brand, read-only.
- **Dashboards** for all brands or a single brand. Each can show all stores or just US or CA.
- **Weekly and monthly reports** compare against the previous period and the same period last year.
- **Frozen share links**: "Share frozen link" on a report creates a URL whose numbers stay fixed until someone presses **Refresh data**. It's the Monday report you can send around.
- **Performance dashboard**: sales, units, search and DSP ad sales and cost, TACOS, sessions, conversion, offer count and Subscribe & Save subscriptions. Columns are Yesterday, Last 7 Days, Month-to-Date and Year-to-Date, each compared with last year.
- **Products are keyed by ASIN.** They're created automatically from order data, and you can edit the names used in reports.
- **Amazon data is append-only.** Every downloaded report is kept, and a new version of an order is stored only when it changes. Nothing is deleted, so reports can always be rebuilt from the raw data.

## Quick start (development)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"

export DJANGO_DEBUG=1
python manage.py migrate
python manage.py seed_demo      # 3 demo brands, fake history for orders, traffic, ads and S&S, demo users
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
- **Saved Seller Central exports**: load a folder from the command line (see below), or upload a single file under Brands → Settings.

Commands:

```bash
python manage.py sync_amazon                     # one sync of every enabled brand
python manage.py sync_amazon --every 3600        # keep syncing hourly (what the worker runs)
python manage.py sync_amazon --backfill-days 400 # load history by order date
python manage.py import_report <brand-slug> <folder> [--create "Brand Name"] [--limit 1]
```

## Performance data (traffic, ads, Subscribe & Save)

The performance dashboard combines several Amazon sources:

| Rows | Source | Amazon access needed |
|---|---|---|
| Sales, Units | Order reports (above) | SP-API, Inventory and Order Tracking role |
| Sessions, Conversion, Total Offer Count | SP-API Sales and Traffic report (`sessions`, units ÷ sessions, `averageOfferCount`) | SP-API **Brand Analytics** role |
| S&S Sub Count | SP-API Replenishment API (`ACTIVE_SUBSCRIPTIONS`) | SP-API **Brand Analytics** role |
| Search Ad Sales / Cost | Amazon Ads API v3 reports: Sponsored Products (7-day attribution) + Brands + Display (14-day) | Separate Ads API app; each brand's ads account authorizes it |
| DSP Ad Sales / Cost | Amazon Ads API DSP reports | As above, plus access to the brand's DSP advertiser |
| TACOS | Total ad cost ÷ sales | |

- **Setup:** set `AMAZON_ADS_CLIENT_ID` / `AMAZON_ADS_CLIENT_SECRET`. On **Brands → Settings**, paste each brand's Ads API refresh token. Advertising profiles for US and CA are looked up automatically. If the brand runs DSP, add its DSP advertiser IDs (`US=…, CA=…`).
- **Storage:** each response is kept as a `DataPull`. The daily tables (`DailyTraffic`, `DailyAds`, `DailySubscriptions`) hold the latest value for each day, because Amazon revises recent days. Each sync re-requests the last 7 days of traffic and S&S data and the last 14 days of ads.
- **History limits:** Amazon only serves sponsored ads for about the last 95 days (Sponsored Display 60) and DSP for 90. Year-to-date and last-year ad numbers therefore build up from the day syncing starts. Connect the Ads API early.
- **Missing or partial data:** traffic arrives about two days late. The dashboard shows "—" when a source has no data for a period, and `*` when it only covers part of the period.
- **Independent sources:** each source syncs on its own. A brand without Brand Analytics or without DSP still gets everything else.
- **Untested:** the real-API code has been written against Amazon's published definitions but not run against Amazon yet. The dummy client returns data in the same shapes.

```bash
python manage.py sync_performance                       # trailing window for every brand
python manage.py sync_performance --days 90 --only ads  # backfill one source
python manage.py sync_performance --every 21600         # what the worker runs (every 6 hours)
```

## Verifying the Amazon integration on a real brand

Once a brand has its SP-API (and optionally Ads API) token saved, run:

```bash
python manage.py verify_amazon <brand-slug> --save-dir verify-output
python manage.py verify_amazon <brand-slug> --only ads --slow   # also probe the ads history limits
python manage.py verify_amazon <brand-slug> --client dummy       # try it out without Amazon
```

It calls the real APIs through the same code the syncs use and checks each assumption the app makes. It prints a checklist (✓ pass, ! warn, ✗ fail, – skipped, i check by hand) and exits with an error if anything fails.

| Group | What it checks |
|---|---|
| orders | The token works. The report has every column we read. `sales-channel` values map to US/CA. Dates parse. Pending orders have no price. Shows an order with quantity > 1 so you can confirm `item-price` is the line total. |
| traffic (per store) | The Brand Analytics role is granted. Every field we read is present. One row per day. How many days late the data arrives. `averageOfferCount` value, to compare with Seller Central. Cross-checks our order data against Amazon's daily units and sales, which tests the `item-price` and time-zone assumptions. |
| subscriptions (per store) | The Replenishment API answers. Daily `activeSubscriptions` come back. Data delay. |
| ads (per store) | The Ads token works. A seller profile exists for each store, in the right currency. SP/SB/SD reports accept our report types and columns. With `--slow`, history really goes back 95/95/60 days. |
| dsp (per store) | The configured advertiser accepts our report. Rows have `date`, `totalCost`, `totalSales`. |

It writes nothing to the database except the advertising profile IDs it looks up. `--save-dir` keeps every raw response, which is the quickest way to fix a parser if Amazon's format differs from what we expect.

## Loading saved exports

Your saved "All Orders" exports are the same report the SP-API sync downloads. Loading them is a way to fill the database with real history and test it before the API is connected.

```bash
python manage.py migrate
python manage.py createsuperuser
python manage.py import_report acme ~/exports/acme --create "Acme Beauty"
```

- **Brand:** `--create` makes the brand, with the US and CA stores, the first time. Leave it off once the brand exists.
- **Order:** files are imported oldest first, as if each one had just been downloaded at that time. That way changes between exports (Pending → Shipped, cancellations) build up the same way the hourly sync records them.
- **Dates:** each file is dated by a timestamp at the start of its name if there is one (e.g. `20250101T120000Z__orders.txt`). Otherwise the newest `last-updated-date` inside the file is used, and failing that, the file's modification time.
- **Files:** folders are searched recursively for `.txt`, `.tsv`, `.csv` and `.gz` files. Both tab-separated (Amazon's format) and comma-separated files work.
- **Re-running:** files already imported are skipped, so it's safe to run again after adding new exports.
- **One file at a time:** `--limit 1` imports just the next file, so you can look at the app between files.
- **Output:** for each file, it prints how many orders were **new**, **changed** or **unchanged** compared with the earlier files.

## Front-end

The styles use Tailwind CSS 4 and daisyUI 5, compiled into `static/css/app.css`, and Chart.js is copied to `static/vendor/`. Both are committed, so running the app doesn't need Node. After changing templates or classes, rebuild with:

```bash
npm install
npm run build        # or: npm run watch
```

## Deployment

```bash
cp .env.example .env          # set DJANGO_SECRET_KEY, FIELD_ENCRYPTION_KEY, hosts…
docker compose up -d --build  # Postgres + web (gunicorn) + order and performance sync workers
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
| `AMAZON_CLIENT` | `dummy` | `dummy` or `sp_api` (applies to orders and performance data) |
| `AMAZON_ADS_CLIENT_ID` / `AMAZON_ADS_CLIENT_SECRET` | | Amazon Ads API app |
| `REPORT_TIME_ZONE` | `America/Los_Angeles` | Day/week/month boundaries and displayed times |
