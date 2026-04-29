FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV CAMPUS_FPM_DB_BACKEND=sqlite
ENV CAMPUS_FPM_DB_PATH=/app/data/campus_fpm.db
ENV CAMPUS_FPM_DB_POOL_SIZE=5

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY static ./static
COPY services ./services
COPY docs ./docs
COPY schema ./schema
COPY migration_proof ./migration_proof
COPY deploy ./deploy
COPY README.md .
COPY docker-compose.yml .
COPY docker-compose.postgres.yml .

EXPOSE 3200

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3200"]
