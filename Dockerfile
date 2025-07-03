# Multi-stage Dockerfile for Snowpark DB-API Transfer Tool
FROM python:3.11-slim as builder

# Set build arguments
ARG TARGETARCH
ARG TARGETPLATFORM

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    unixodbc-dev \
    gnupg2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies first (most stable)
# --no-cache-dir is not needed here because we are not using a cache
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Testing stage - Dedicated for running tests
FROM python:3.11-slim as testing

# Create non-root user for testing
RUN groupadd -g 1000 testuser && \
    useradd -r -u 1000 -g testuser -m -d /home/testuser testuser

# Install runtime dependencies and Microsoft ODBC Driver
RUN apt-get update && apt-get install -y \
    unixodbc \
    curl \
    gnupg2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server - Fixed approach
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/* || echo "ODBC driver installation failed - will work without SQL Server support"

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy application code for testing
COPY snowpark_db_api/ ./snowpark_db_api/
COPY tests/ ./tests/
COPY run_tests.py pytest.ini ./
COPY pyproject.toml setup.py settings.ini ./

# Install the package in development mode for testing
RUN pip install -e . || echo "Package installation failed - continuing"

# Create test directories and set permissions
RUN mkdir -p /app/logs /app/test-results /app/htmlcov \
    && chown -R testuser:testuser /app

# Switch to test user
USER testuser

# Set environment variables for testing
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTEST_CURRENT_TEST=1

# Health check for testing image
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=2 \
    CMD python -c "import sys; sys.path.append('.'); import snowpark_db_api; print('Testing image OK')" || exit 1

# Default command for testing
CMD ["python", "run_tests.py", "smoke"]

# Production stage
FROM python:3.11-slim as production

# Create non-root user
RUN groupadd -g 1000 appuser && \
    useradd -r -u 1000 -g appuser -m -d /home/appuser appuser && \
    mkdir -p /home/appuser/.local/share/jupyter && \
    chown -R appuser:appuser /home/appuser

# Install runtime dependencies and Microsoft ODBC Driver
RUN apt-get update && apt-get install -y \
    unixodbc \
    curl \
    gnupg2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server - Fixed approach
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/* || echo "ODBC driver installation failed - will work without SQL Server support"

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy application code
COPY snowpark_db_api/ ./snowpark_db_api/
COPY nbs/ ./nbs/
COPY config/ ./config/
COPY tests/ ./tests/
COPY run_tests.py pytest.ini ./
COPY pyproject.toml setup.py settings.ini ./

# Install the package in development mode
RUN pip install -e . || echo "Package installation failed - continuing"

# Create directories for logs and config
RUN mkdir -p /app/logs /app/config /app/data \
    && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose Jupyter port for development
EXPOSE 8888
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.path.append('.'); import snowpark_db_api; print('OK')" || exit 1

# Default command for Jupyter Lab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--notebook-dir=/app/nbs", "--NotebookApp.token=''", "--NotebookApp.password=''"] 