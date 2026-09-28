FROM python:3.12-slim

WORKDIR /test_docker

# Copy dependencies first so Docker can cache this layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Then copy the rest of the code
COPY app.py .

EXPOSE 8080
CMD ["python3", "app.py"]
