FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md requirements.md schema.sql sources.yaml config.py ./
COPY apps ./apps
COPY collectors ./collectors
COPY loaders ./loaders
COPY parsers ./parsers
COPY pipelines ./pipelines
COPY sql ./sql
COPY validators ./validators

RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir .

EXPOSE 8000

CMD ["sh", "-c", "python -m pipelines.init_db && uvicorn apps.api.main:app --host 0.0.0.0 --port 8000"]
