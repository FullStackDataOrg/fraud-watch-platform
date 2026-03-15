FROM python:3.11-slim
WORKDIR /app

COPY serving/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# shared feature code
COPY features.py .
COPY serving/main.py .
COPY serving/predictor.py .
COPY serving/feature_client.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
