from typing import List, Dict
from src.utils import logger
from src.utils import Base, get_engine
from sqlalchemy import Column, String, Float, DateTime, Integer, UniqueConstraint, text


class WeatherData(Base):
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True)
    city = Column(String, nullable=False)
    country = Column(String, nullable=False)
    longitude = Column(Float, nullable=False)
    latitude = Column(Float, nullable=False)
    temperature = Column(Float, nullable=False)
    humidity = Column(Float, nullable=False)
    pressure = Column(Float, nullable=False)
    wind_speed = Column(Float, nullable=False)
    wind_direction = Column(Float, nullable=False)
    observed_at = Column(DateTime, nullable=False)
    provider = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "city", "country", "observed_at", name="unique_city_country_observed_at"
        ),
    )


def write_to_postgresql(data: List[Dict]) -> None:
    """Create table if not exists and bulk insert data, avoiding duplicates."""
    try:
        engine = get_engine()
        Base.metadata.create_all(engine)

        if data:
            insert_stmt = """
            INSERT INTO weather_data (city, country, longitude, latitude, temperature, humidity, pressure, wind_speed, wind_direction, observed_at, provider)
            VALUES (:city, :country, :longitude, :latitude, :temperature, :humidity, :pressure, :wind_speed, :wind_direction, :observed_at, :provider)
            ON CONFLICT (city, country, observed_at) DO NOTHING
            """
            with engine.connect() as conn:
                result = conn.execute(text(insert_stmt), data)
                conn.commit()
                rows_inserted = result.rowcount
                rows_skipped = len(data) - rows_inserted
                logger.info(f"Inserted {rows_inserted} records, skipped {rows_skipped} duplicates into PostgreSQL.")
    except Exception as e:
        logger.info(f"Error writing to PostgreSQL: {e}")


def load_to_postgresql(data: List[Dict]) -> None:
    """Main function to load transformed data to PostgreSQL."""
    write_to_postgresql(data)
