# Use a lightweight Python image
FROM python:3.8-slim

# Set working directory in container
WORKDIR /app

# Copy requirements first, for caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY main2.py .

# Default command (change as needed if script has a different name)
CMD ["python", "codes/2_task2/main2.py"]

