# Use Python 3.8 slim image
FROM python:3.8-slim

# Set working directory
WORKDIR /app

# Copy dependencies and source
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code:
COPY main1.py .

# Set default entrypoint for CLI-style execution
ENTRYPOINT ["python", "main1.py"]
