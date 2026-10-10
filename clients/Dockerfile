FROM python:3.12-slim

WORKDIR /app

# Copy dependencies first so Docker can cache this layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY app.py .

EXPOSE 8080
CMD ["python", "app.py"]
