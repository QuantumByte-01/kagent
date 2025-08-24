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


RUN PIP_EXTRA_INDEX_URL=https://download.pytorch.org/whl/cpu \
    UV_PIP_EXTRA_INDEX_URL=https://download.pytorch.org/whl/cpu \
    UV_HTTP_TIMEOUT=300 \
    UV_NO_CACHE=1 \
    uv sync --no-dev --frozen

# Copy the backend source code
COPY backend/ ./

EXPOSE 8000


# Run the backend API
CMD ["uv", "run", "main.py", "--host", "0.0.0.0", "--port", "8000"]
