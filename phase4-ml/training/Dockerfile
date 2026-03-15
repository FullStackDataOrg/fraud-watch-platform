FROM python:3.11-slim
WORKDIR /app

COPY training/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# shared feature code
COPY features.py .
COPY training/train.py .
COPY training/evaluate.py .

CMD ["python", "train.py"]
