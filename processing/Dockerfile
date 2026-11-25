FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# curl is only needed for the Compose healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first so the dependency layer is cached between code changes
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8100
CMD ["uvicorn", "processing:app", "--host", "0.0.0.0", "--port", "8100"]
