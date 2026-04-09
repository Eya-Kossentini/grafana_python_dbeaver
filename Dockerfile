# Builder stage: install deps, compile to .pyc, remove sources
FROM python:3.10-buster AS builder

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install dependencies and build wheels
COPY requirements.txt ./
RUN pip install --upgrade pip \
 && pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# Copy source and compile to bytecode (.pyc alongside)
COPY . .
# -OO to strip docstrings and asserts; -f forces recompilation
# NOTE: Keep .py sources so the app can be imported reliably
RUN python -OO -m compileall -f .

# Runtime stage: copy only wheels and compiled app
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8011

WORKDIR /app

# Keep requirements for image transparency
COPY requirements.txt ./requirements.txt

# Install from prebuilt wheels to avoid fetching from internet at runtime
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
 && rm -rf /wheels

# Copy compiled application only
COPY --from=builder /app /app

# Run as non-root user
RUN useradd -m -u 10001 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8011

CMD ["uvicorn", "application:app", "--host", "0.0.0.0", "--port", "8011"]