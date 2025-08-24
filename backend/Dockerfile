FROM python:3.12-slim

# Ensure Python output is sent straight to the terminal without buffering
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_ENV=production

WORKDIR /app

# Install the uv package manager without caching to keep the image small
RUN pip install --no-cache-dir uv

# Copy project dependency descriptors
COPY pyproject.toml uv.lock .env ./

# Copy the service account for Google Cloud access
COPY backend/service-account.json ./service-account.json

# Install only production dependencies with a longer timeout and CPU-only PyTorch wheels
RUN PIP_INDEX_URL=https://download.pytorch.org/whl/cpu \
    UV_PIP_INDEX_URL=https://download.pytorch.org/whl/cpu \
    UV_HTTP_TIMEOUT=300 \
    UV_NO_CACHE=1 \
    uv sync --no-dev --frozen

# Copy the backend source code
COPY backend/ ./

EXPOSE 8000

# Point the Google credentials to the copied service account
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/service-account.json

# Run the backend API
CMD ["uv", "run", "main.py", "--host", "0.0.0.0", "--port", "8000"]
