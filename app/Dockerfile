FROM python:3.11-slim

WORKDIR /app

COPY requirements_dashboard.txt .
RUN pip install --no-cache-dir -r requirements_dashboard.txt

COPY . .

EXPOSE 8501

# The DuckDB warehouse is mounted at /warehouse.duckdb at run time.
ENV DUCKDB_PATH=/warehouse.duckdb

ENTRYPOINT ["streamlit", "run", "dashboard.py", \
            "--server.port=8501", "--server.address=0.0.0.0", \
            "--server.headless=true"]
