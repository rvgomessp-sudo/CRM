FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg/asyncpg/cryptography
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY backend/ ./backend/
COPY alembic.ini ./alembic.ini

# Railway uses $PORT env var
ENV PORT=8000
EXPOSE 8000

# Run migrations then start server
CMD alembic upgrade head && uvicorn backend.main:app --host 0.0.0.0 --port ${PORT}
