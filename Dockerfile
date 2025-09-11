# --- Builder Stage ---
FROM python:3.11-alpine as builder

# Install build dependencies only
RUN apk add --no-cache gcc musl-dev libffi-dev postgresql-dev

WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt && \
    find /root/.local -type f -name "*.pyc" -delete && \
    find /root/.local -type d -name "__pycache__" -delete && \
    find /root/.local -type f -name "*.pyi" -delete && \
    find /root/.local -type f -name "*.pyx" -delete

# --- Final Stage ---
FROM python:3.11-alpine

# Install only runtime dependencies
RUN apk add --no-cache libpq curl && \
    adduser -D -u 1000 atabot

# Copy Python packages
COPY --from=builder --chown=atabot:atabot /root/.local /home/atabot/.local

# Copy only necessary app files
WORKDIR /app
COPY --chown=atabot:atabot ./app ./app

# Remove unnecessary files
RUN find /app -type f -name "*.md" -delete && \
    find /app -type f -name "*.txt" -delete && \
    find /app -type f -name "*.pyc" -delete && \
    find /app -type d -name "__pycache__" -delete && \
    find /app -type d -name ".git" -delete

USER atabot
ENV PATH="/home/atabot/.local/bin:${PATH}"

EXPOSE 8000

# Use single worker to reduce memory
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--loop", "asyncio"]