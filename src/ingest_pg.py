import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from .mongo_client import get_collection

load_dotenv()

def get_pg_engine():
    user = os.getenv("PG_USER")
    pw = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    db = os.getenv("PG_DB")  

    connection_string = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(connection_string)

def sync_mongo_to_pg():
    # 1. Fetch from Mongo
    col = get_collection()
    mongo_docs = list(col.find())
    
    if not mongo_docs:
        print("No data in MongoDB to sync.")
        return

    # 2. Flatten and Transform
    rows = []

    for doc in mongo_docs:
        city_name = doc.get("city")
        
        if not city_name:
            print(f"Skipping document with ID {doc.get('_id')} because 'city' is missing.")
            continue 

        metrics = doc.get("metrics", {})
        coords = doc.get("coordinates", {})
        wind = doc.get("wind", {}) 

        # ✅ SAFE TYPE CASTING FUNCTION
        def to_float(val):
            try:
                return float(val) if val is not None else None
            except (ValueError, TypeError):
                return None

        pg_data = {
            "city": city_name,
            "country": doc.get("country_code"),
            "longitude": to_float(coords.get("lon")),
            "latitude": to_float(coords.get("lat")),
            "temperature": to_float(metrics.get("temp")),
            "humidity": to_float(metrics.get("humidity")),
            "pressure": to_float(metrics.get("pressure")),
            "wind_speed": to_float(wind.get("speed")),
            "wind_direction": to_float(wind.get("deg")),
            "observed_at": doc.get("observed_at"),
            "provider": doc.get("provider")
        }
        
        rows.append(pg_data)
    
    if not rows:
        print("No valid records found to sync.")
        return

    # 3. Write to Postgres
    df = pd.DataFrame(rows)

    # ✅ ENSURE DATAFRAME TYPES (CRITICAL)
    numeric_cols = [
        "longitude", "latitude", "temperature",
        "humidity", "pressure", "wind_speed", "wind_direction"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    engine = get_pg_engine()
    
    # Create temp table
    df.to_sql("temp_weather", engine, if_exists="replace", index=False)
    
    # ✅ DEFENSIVE SQL CASTING (extra safety)
    upsert_query = """
    INSERT INTO weather_observations (
        city, country, longitude, latitude, temperature, 
        humidity, pressure, wind_speed, wind_direction, 
        observed_at, provider
    )
    SELECT 
        city, 
        country, 
        longitude::double precision, 
        latitude::double precision, 
        temperature::double precision, 
        humidity::double precision, 
        pressure::double precision, 
        wind_speed::double precision, 
        wind_direction::double precision, 
        observed_at, 
        provider 
    FROM temp_weather
    ON CONFLICT (city, country, observed_at) DO NOTHING;
    """
    
    with engine.connect() as conn:
        conn.execute(text(upsert_query))
        conn.execute(text("DROP TABLE temp_weather;"))
        conn.commit()
        
    print(f"Successfully processed {len(df)} records for PostgreSQL sync.")
