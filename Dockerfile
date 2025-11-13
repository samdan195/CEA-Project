# Use official lightweight Python image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy all files to container
COPY . .

# Ensure folders exist (optional but safe)
RUN mkdir -p data/incoming data/archive data/rejected logs app samples

# Install any dependencies (if needed later)
# RUN pip install -r requirements.txt

# Default command to run the validator
ENTRYPOINT ["python", "app.py"]
