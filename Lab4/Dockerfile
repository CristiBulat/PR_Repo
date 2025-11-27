# Lab 4: Key-Value Store with Single-Leader Replication
# This Dockerfile creates an image that can run as either leader or follower

FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
# Flask for HTTP API, requests for leader->follower communication
RUN pip install --no-cache-dir flask requests

# Copy application files
COPY kv_store.py /app/
COPY leader.py /app/
COPY follower.py /app/

# Set unbuffered output for cleaner logs
ENV PYTHONUNBUFFERED=1

# Default port (will be overridden by docker-compose)
EXPOSE 5000

# The command will be specified in docker-compose.yml
# Either: python leader.py (for leader)
# Or: python follower.py (for followers)
CMD ["python", "leader.py"]