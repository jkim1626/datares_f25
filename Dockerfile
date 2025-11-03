# Use a small, recent Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies efficiently
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only your source code (respecting .dockerignore)
COPY . .

# Default command — can be overridden in Railway UI (Start Command)
CMD ["python", "run_all.py"]
