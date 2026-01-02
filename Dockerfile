FROM python:3.11-slim

WORKDIR /app

# App writes all state under XDG_DATA_HOME via aws_reports.userdirs.user_data_dir()
# which becomes: $XDG_DATA_HOME/aws-reporting
ENV XDG_DATA_HOME=/data
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Runtime deps (from pyproject.toml)
RUN pip install --no-cache-dir flask flask-login docopt

COPY aws_reports/ ./aws_reports/

# Persist all app data (users.sqlite, brands.json, per-brand orders.sqlite, tmp_uploads)
VOLUME ["/data"]

EXPOSE 8080

CMD ["python", "-c", "from aws_reports.app import app; app.run(host='0.0.0.0', port=8080, debug=False)"]

