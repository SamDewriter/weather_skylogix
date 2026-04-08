from typing import List, Dict
from src.utils import logger
from src.utils import get_collection


def get_weather_data_from_mongo() -> List[Dict]:
    """Fetch all weather data from MongoDB collection."""
    try:
        collection = get_collection()
        data = list(collection.find({}))
        logger.info(f"Fetched {len(data)} records from MongoDB.")
        return data
    except Exception as e:
        logger.info(f"Error fetching data from MongoDB: {e}")
        return []


def transform_weather_data(mongo_data: List[Dict]) -> List[Dict]:
    """
    Transform MongoDB weather documents into flat dicts
    suitable for bulk PostgreSQL insertion.

    Output columns:
    city, country, longitude, latitude, temperature, humidity,
    pressure, wind_speed, wind_direction, observed_at, provider
    """
    return [
        {
            "city": doc.get("city"),
            "country": doc.get("country_code"),
            "longitude": doc.get("coordinates", {}).get("lon"),
            "latitude": doc.get("coordinates", {}).get("lat"),
            "temperature": doc.get("metrics", {}).get("temperature"),
            "humidity": doc.get("metrics", {}).get("humidity"),
            "pressure": doc.get("metrics", {}).get("pressure"),
            "wind_speed": doc.get("metrics", {}).get("wind_speed"),
            "wind_direction": doc.get("metrics", {}).get("wind_direction"),
            "observed_at": doc.get("observed_at"),
            "provider": doc.get("provider"),
        }
        for doc in mongo_data
    ]


def transform_staged_data() -> List[Dict]:
    """Fetch staged data from MongoDB, transform it, and return the transformed list."""
    mongo_data = get_weather_data_from_mongo()
    transformed_data = transform_weather_data(mongo_data)
    logger.info(f"Transformed {len(transformed_data)} weather records.")
    return transformed_data
