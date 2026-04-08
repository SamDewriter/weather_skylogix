from typing import List, Optional, Dict
from src.utils import logger
from src.utils import get_collection
from src.utils import fetch_weather
from src.utils import normalize_weather_data
from pymongo import UpdateOne, ASCENDING, errors as pymongo_errors
import geonamescache


def get_city_dicts(city_names: List[str]) -> List[Dict[str, str]]:
    """Convert a list of city names to city dicts with country codes using geonamescache.
    
    Args:
        city_names: List of city name strings
        
    Returns:
        List of dicts with 'city' and 'country_code' keys
    """
    gc = geonamescache.GeonamesCache()
    cities = []
    
    for city_name in city_names:
        matches = gc.search_cities(city_name, contains_search=False)
        
        if matches:
            matches.sort(key=lambda x: x['population'], reverse=True)
            best_match = matches[0]
            
            cities.append({
                "city": city_name,
                "country_code": best_match['countrycode']
            })
        else:
            logger.warning(f"No geonamescache match found for city: {city_name}")
    
    return cities


def ensure_indexes():
    col = get_collection()
    col.create_index("updatedAt")
    col.create_index([("city", ASCENDING), ("observed_at", ASCENDING)])


def ingest_once(cities: List[dict]):
    col = get_collection()

    operations = []

    for c in cities:
        city = c["city"]
        country_code = c["country_code"]
        logger.info(f"Fetching weather for {city}, {country_code}...")

        try:
            raw_data = fetch_weather(city, country_code)
            normalized_doc = normalize_weather_data(raw_data, city, country_code)

            filter_query = {
                "city": city,
                "country_code": country_code,
                "observed_at": normalized_doc["observed_at"],
            }

            update_doc = {"$set": normalized_doc, "$currentDate": {"updatedAt": True}}
            operations.append(UpdateOne(filter_query, update_doc, upsert=True))
        except Exception as e:
            logger.info(f"Error fetching or normalizing data for {city}, {country_code}: {e}")

    if operations:
        try:
            result = col.bulk_write(operations)
            logger.info(f"Bulk write result: {result.bulk_api_result}")
        except pymongo_errors.BulkWriteError as bwe:
            logger.info(f"Bulk write error: {bwe.details}")
        except Exception as e:
            logger.info(f"Error during bulk write: {e}")


def stage_weather_data(cities: Optional[List[str]] = None) -> None:
    """Ingest and stage weather data, ensuring indexes are in place for efficient upserts.
    
    Args:
        cities: Optional list of city name strings. If None, defaults to ['Port Harcourt']
    """
    ensure_indexes()
    
    if cities is None:
        cities = ["Port Harcourt"]
    
    cities_to_ingest = get_city_dicts(cities)
    
    if not cities_to_ingest:
        logger.info("No valid cities found for ingestion.")
        return
    
    ingest_once(cities_to_ingest)
