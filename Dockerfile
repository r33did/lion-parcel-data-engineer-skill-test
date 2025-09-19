FROM python:3.11-slim

WORKDIR /app

# Install dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_job.py .
COPY .env .

# Wrapper script to run ETL in a loop (for debugging now)
CMD ["sh", "-c", "while true; do python /app/etl_job.py || echo 'ETL failed, retrying in 60s'; sleep 60; done"]
