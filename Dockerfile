# --- Builder Stage ---
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
# Install packages to be copied to the non-root user's home directory later
RUN pip install --no-cache-dir --user -r requirements.txt

# --- Final Stage ---
FROM python:3.11-slim

WORKDIR /app

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create the non-root user first
RUN useradd -m -u 1000 atabot

# Copy Python packages from builder to the atabot user's home directory
# and set the correct ownership at the same time.
COPY --from=builder --chown=atabot:atabot /root/.local /home/atabot/.local

# Copy application code and set ownership
COPY --chown=atabot:atabot ./app ./app

# Switch to the non-root user
USER atabot

# Add the user's local bin directory to the PATH
ENV PATH="/home/atabot/.local/bin:${PATH}"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health/health || exit 1

# Expose port
EXPOSE 8000

# Run with optimized settings
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--loop", "uvloop", \
     "--access-log", \
     "--log-level", "info"]