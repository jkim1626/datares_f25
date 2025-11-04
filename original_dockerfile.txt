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

# Create data directory (Railway volume will mount over this)
RUN mkdir -p /data/visa_stats

# Run initialization and scraping
CMD python init_db.py && python run_all.py