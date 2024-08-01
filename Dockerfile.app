FROM python:3.8

# Install dependencies
RUN pip3 install --upgrade pip
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Copy the application code to the container
COPY . /app

# Set the working directory
WORKDIR /app

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
