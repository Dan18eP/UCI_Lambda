FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY icu_telemetry_generator.py .

CMD ["python", "-u", "icu_telemetry_generator.py"]
