FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (for psycopg)
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DATA_ROOT=/data

# Run TEST pipeline (change to run_all.py for full load)
# For testing: CMD python test_run_all.py
# For full load: CMD python run_all.py
CMD python run_all.py