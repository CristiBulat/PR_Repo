# Use a slim Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy application files
COPY server.py /app/
COPY collection/ /app/collection/

# Expose port 8080
EXPOSE 8080

# Set unbuffered output for cleaner logs
ENV PYTHONUNBUFFERED=1

# Run the server
CMD ["python", "-u", "server.py", "collection"]