# Use official Python runtime as base image
FROM python:3.10-slim

# Set working directory in container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port (Render uses dynamic PORT)
EXPOSE 10000

# Set environment variables
ENV FLASK_APP=api.py
ENV PYTHONUNBUFFERED=1

# Run the application with gunicorn (production-ready)
CMD gunicorn --bind 0.0.0.0:${PORT:-5001} --workers 2 --threads 4 api:app
