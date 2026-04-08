# SkyLogix2 — Weather Data Pipeline

A Python ETL pipeline that fetches live weather data from the OpenWeatherMap API, stores it in MongoDB, and writes cleaned records to PostgreSQL.

---

## Pipeline Overview

```
OpenWeatherMap API → MongoDB (raw + normalized) → PostgreSQL (clean, deduplicated)
```

1. **Stage** — `src/data/ingest_data.py` fetches weather data for specified cities, normalizes it, and upserts into MongoDB, deduplicating on `city + country_code + observed_at`.
2. **Transform** — `src/data/transform_data.py` reads from MongoDB and flattens the documents into a format suitable for PostgreSQL.
3. **Load** — `src/data/load_to_pg.py` inserts the transformed data into PostgreSQL, skipping duplicates on `city + country + observed_at`.

---

## PostgreSQL Schema

Table: `weather_data`

| Column | Type | Description |
|---|---|---|
| city | TEXT | City name |
| country | TEXT | Country code (e.g. `US`, `GB`) |
| longitude | FLOAT | Geographic longitude |
| latitude | FLOAT | Geographic latitude |
| temperature | FLOAT | Temperature (Kelvin by default) |
| humidity | FLOAT | Relative humidity (%) |
| pressure | FLOAT | Atmospheric pressure (hPa) |
| wind_speed | FLOAT | Wind speed (m/s) |
| wind_direction | FLOAT | Wind direction (degrees) |
| observed_at | TIMESTAMP | Observation timestamp (UTC) |
| provider | TEXT | Data source (e.g. `openweathermap`) |

Unique constraint: `(city, country, observed_at)`

--- 

## Setup

*Ensure the `uv` package manager is installed on target machine*

1. Copy `env.example` to `.env` and fill in your credentials.
2. Sync project dependencies:
   ```bash
   uv sync
   ```
3. Run the pipeline:
   ```bash
   uv run main.py --cities 'New York' London Tokyo
   ```
---

## Project Structure

```
weather-skylogix/
├── main.py                  # Entry point
├── pyproject.toml           # Project dependencies and configuration
├── env.example              # Environment variables template
├── README.md
├── explorations/
│   └── exp.ipynb            # Jupyter notebook for explorations
└── src/
    ├── data/
    │   ├── __init__.py
    │   ├── ingest_data.py   # Fetch and stage weather data to MongoDB
    │   ├── load_to_pg.py    # Load transformed data to PostgreSQL
    │   └── transform_data.py # Transform MongoDB data for PostgreSQL
    └── utils/
        ├── __init__.py
        ├── logging.py        # Logging utilities
        ├── mongo_client.py   # MongoDB connection
        ├── normalize.py      # Data normalization
        ├── postgresdb_client.py # PostgreSQL connection
        └── weather_client.py # OpenWeatherMap API client
```
