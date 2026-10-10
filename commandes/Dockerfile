FROM python:3.12-slim

WORKDIR /app

# Copy dependencies first so Docker can cache this layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Then copy the rest of the code
COPY app.py .

EXPOSE 8080
CMD ["python", "app.py"]
