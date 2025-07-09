# Financial Transactions ETL + API

ETL pipeline (Airflow) processes CSV transactions into PostgreSQL. FastAPI serves async transaction summaries.

**Quick start:** `docker-compose up -d` → Airflow UI: http://localhost:8080 (admin/admin) → API: http://localhost:8000/docs

**Test:** `curl http://localhost:8000/transactions/1034/summary`