from src.data import (
    stage_weather_data,
    transform_staged_data,
    load_to_postgresql,
)


if __name__ == "__main__":
    stage_weather_data()
    transformed_data = transform_staged_data()
    load_to_postgresql(transformed_data)
