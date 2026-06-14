[**Read the Full Article**](https://oyetoluzeenat.github.io/portfolio/sky-logix-weather-pipeline/)


# SkyLogix — Weather Data Pipeline

![SkyLogix Weather Data Pipeline](./weather_skylogix_pipeline.png)

SkyLogix is a Python-based data pipeline designed to ingest, normalize, and store global weather metrics. The system fetches raw weather data from an external API, manages unstructured data stores, processes and normalizes the metrics, and loads the structured data into a relational database for downstream analytics.

## Tech Stack & Tools
* **Language:** Python 3.x
* **Data Storage:** MongoDB (Raw/Unstructured Landing), PostgreSQL (Structured/Analytical Store)
* **Environment Management:** Dotenv

---

## Project Structure

```text
weather_skylogix_zeenat/
│
├── src/
│   ├── __init__.py
│   ├── weather_client.py    # Fetches data from the external weather API
│   ├── mongo_client.py      # Manages connection and storage for MongoDB
│   ├── ingest_weather.py    # Orchestrates raw data ingestion into MongoDB
│   ├── normalize.py         # Transforms and cleans raw JSON to structured data
│   └── ingest_pg.py         # Loads normalized data into PostgreSQL
│
├── .env.example             # Template for required environment variables
├── .gitignore               # Ensures sensitive files aren't tracked
├── .exp.ipynb               # Jupyter Notebook for scratchpad/experimentation
├── main.py                  # Pipeline entry point to run the execution flow
└── requirement.text          # Project dependencies
```

---

## Getting Started

### 1. Prerequisites
Ensure you have Python 3.x installed, along with running instances of **MongoDB** and **PostgreSQL**.

### 2. Installation
Clone the repository and navigate to the project root:
```bash
git clone https://github.com/your-username/weather_skylogix_zeenat.git
cd weather_skylogix_zeenat
```

Install the required dependencies:
```bash
pip install -r requirement.text
```

### 3. Configuration
Create a `.env` file in the root directory based on the provided example:
```bash
cp .env.example .env
```

Open `.env` and fill in your specific credentials:
```env
WEATHER_API_KEY=your_api_key_here
MONGO_URI=mongodb://localhost:27017/your_db
POSTGRES_URI=postgresql://user:password@localhost:5432/your_db
```

---

## How it Works & Running the Pipeline

The pipeline follows a modular ETL workflow:
1. **Extract:** `weather_client.py` requests the live metrics.
2. **Load (Raw):** `ingest_weather.py` dumps the raw JSON payloads into MongoDB via `mongo_client.py`.
3. **Transform:** `normalize.py` extracts, flattens, and cleans the target weather metrics.
4. **Load (Structured):** `ingest_pg.py` writes the clean records into PostgreSQL.

To run the entire end-to-end pipeline, execute the main script:
```bash
python main.py
```