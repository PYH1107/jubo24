FROM python:3.7.12-slim

# Install necessary system packages
RUN apt-get update && apt-get install -y \
    python3-pip \
    build-essential \
    libpq-dev \
    git \
    apache2-utils \
    poppler-utils \
    && apt-get clean

# Upgrade pip and install Python dependencies
RUN pip3 install --upgrade pip
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Copy the application code to the container
COPY . /app
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app

# Expose the port FastAPI will run on
EXPOSE 8000

