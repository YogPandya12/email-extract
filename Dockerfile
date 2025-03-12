# Use a slim Python base image based on Debian Bookworm
FROM python:3.11-slim-bookworm

# Set working directory
WORKDIR /app

# Install basic system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    curl \
    unzip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome and its dependencies in one step
RUN apt-get update && apt-get install -y \
    wget \
    libxss1 \
    libnss3 \
    libpango-1.0-0 \
    libxtst6 \
    fonts-liberation \
    libx11-xcb1 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libasound2 \
    && wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 5000

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the application with Gunicorn
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]