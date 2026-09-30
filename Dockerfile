FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data

COPY pyproject.toml README.md ./
COPY config/ ./config/
COPY apps/ ./apps/
RUN pip install --no-cache-dir .

COPY manage.py ./
COPY templates/ ./templates/
# static/css/app.css is prebuilt (npm run build) and committed, so no Node here.
COPY static/ ./static/
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# SQLite DB (when used) and raw report files live here.
VOLUME ["/data"]
EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8080", "--workers", "3", "--timeout", "120"]
