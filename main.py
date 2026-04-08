import argparse
from src.data import (
    stage_weather_data,
    transform_staged_data,
    load_to_postgresql,
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Weather data ingestion pipeline"
    )
    parser.add_argument(
        "--cities",
        nargs="+",
        type=str,
        help="Specific cities to fetch weather for (e.g., --cities 'New York' London Tokyo)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    if args.cities:
        print(f"Targeting cities: {', '.join(args.cities)}")
    
    stage_weather_data(cities=args.cities)
    transformed_data = transform_staged_data()
    load_to_postgresql(transformed_data)
