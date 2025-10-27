import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

print("🚀 Starting build_silver_table.py")

# 🔐 PostgreSQL connection settings
DB_CONFIG = {
    "user": "postgres",
    "password": "Abayomi01@",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "nyc_taxi_pipeline"
}

encoded_password = quote_plus(DB_CONFIG["password"])
engine_url = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)
engine = create_engine(engine_url, connect_args={"host": "127.0.0.1"})
print("✅ Connected to PostgreSQL")

# 🧩 Load and transform in chunks
chunk_size = 500_000
query = "SELECT * FROM raw_taxi_data"
chunks = pd.read_sql(query, engine, chunksize=chunk_size)

for i, df in enumerate(chunks):
    print(f"\n🔄 Processing chunk {i+1}...")

    df_silver = df[[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type"
    ]].copy()

    df_silver.columns = [
        "pickup_time",
        "dropoff_time",
        "passengers",
        "distance_miles",
        "fare_usd",
        "tip_usd",
        "total_usd",
        "payment_type"
    ]

    df_silver = df_silver.dropna()
    df_silver = df_silver[df_silver["distance_miles"] > 0]
    df_silver = df_silver[df_silver["passengers"] > 0]

    df_silver["trip_duration_minutes"] = (
        pd.to_datetime(df_silver["dropoff_time"]) - pd.to_datetime(df_silver["pickup_time"])
    ).dt.total_seconds() / 60

    df_silver.to_sql("silver_taxi_data", engine, if_exists="append", index=False)
    print(f"✅ Chunk {i+1} saved to 'silver_taxi_data'")

print("\n🎉 Finished building Silver table.")

import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

print("🚀 Starting build_silver_table.py")

# 🔐 PostgreSQL connection settings
DB_CONFIG = {
    "user": "postgres",
    "password": "Abayomi01@",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "nyc_taxi_pipeline"
}

encoded_password = quote_plus(DB_CONFIG["password"])
engine_url = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)
engine = create_engine(engine_url, connect_args={"host": "127.0.0.1"})
print("✅ Connected to PostgreSQL")

# 🧩 Load and transform in chunks
chunk_size = 500_000
query = "SELECT * FROM raw_taxi_data"
chunks = pd.read_sql(query, engine, chunksize=chunk_size)

for i, df in enumerate(chunks):
    print(f"\n🔄 Processing chunk {i+1}...")

    df_silver = df[[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type"
    ]].copy()

    df_silver.columns = [
        "pickup_time",
        "dropoff_time",
        "passengers",
        "distance_miles",
        "fare_usd",
        "tip_usd",
        "total_usd",
        "payment_type"
    ]

    df_silver = df_silver.dropna()
    df_silver = df_silver[df_silver["distance_miles"] > 0]
    df_silver = df_silver[df_silver["passengers"] > 0]

    df_silver["trip_duration_minutes"] = (
        pd.to_datetime(df_silver["dropoff_time"]) - pd.to_datetime(df_silver["pickup_time"])
    ).dt.total_seconds() / 60

    df_silver.to_sql("silver_taxi_data", engine, if_exists="append", index=False)
    print(f"✅ Chunk {i+1} saved to 'silver_taxi_data'")

print("\n🎉 Finished building Silver table.")
