FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py check_client.py ./
COPY config/tags.yaml /config/tags.yaml

ENV CONFIG_PATH=/config/tags.yaml \
    PYTHONUNBUFFERED=1

EXPOSE 49320

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import socket; socket.create_connection(('127.0.0.1', 49320), 2).close()"

CMD ["python", "server.py"]
