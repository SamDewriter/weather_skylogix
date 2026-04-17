from src.ingest_weather import ingest_once, ensure_indexes
from src.ingest_pg import sync_mongo_to_pg

if __name__ == "__main__":
    # Step 1: Ensure Mongo is ready and fetch API data
    print("--- Starting Mongo Ingestion ---")
    ensure_indexes()
    ingest_once(None)
    
    # Step 2: Ensure Postgres is ready and sync data
    print("\n--- Starting PostgreSQL Sync ---")
  
    sync_mongo_to_pg()
    
    print("\nPipeline execution complete.")
