# API ETL Integration (Postgres)
**REST/JSON + SOAP/XML → pandas → AWS RDS PostgreSQL (with screenshots)**

**Stack:** Python (requests, pandas, zeep, SQLAlchemy), AWS RDS PostgreSQL, DBeaver

## What it does
- **Extracts**: Weather (REST/JSON via OpenWeather) + Country info (SOAP/XML demo service).
- **Transforms**: Normalizes to tidy DataFrames in pandas.
- **Loads**: Writes to AWS RDS Postgres tables `weather_data`, `country_info`, and a joined view `vw_weather_country`.

## Run locally
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

