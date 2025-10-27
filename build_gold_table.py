<<<<<<< HEAD
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

print("🚀 Starting build_gold_table.py")

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

# 🧠 Aggregation query
query = """
SELECT
    DATE_TRUNC('month', pickup_time) AS month,
    COUNT(*) AS total_trips,
    SUM(total_usd) AS total_revenue,
    AVG(distance_miles) AS avg_distance,
    AVG(fare_usd) AS avg_fare,
    AVG(tip_usd) AS avg_tip,
    AVG(tip_usd / NULLIF(fare_usd, 0)) * 100 AS avg_tip_pct,
    SUM(passengers) AS total_passengers
FROM silver_taxi_data
GROUP BY month
ORDER BY month;
"""

try:
    df_gold = pd.read_sql(query, engine)
    print(f"📊 Aggregated {len(df_gold)} monthly rows from 'silver_taxi_data'")
    print("🔍 Preview of Gold metrics:")
    print(df_gold.head())

    df_gold.to_sql("gold_taxi_metrics", engine, if_exists="replace", index=False)
    print("✅ Gold table 'gold_taxi_metrics' created successfully.")

    # 📤 Export to CSV
    df_gold.to_csv("gold_taxi_metrics.csv", index=False)
    print("📁 Exported Gold metrics to 'gold_taxi_metrics.csv'")

except Exception as e:
    print(f"❌ Error building Gold table: {e}")
=======
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

print("🚀 Starting build_gold_table.py")

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

# 🧠 Aggregation query
query = """
SELECT
    DATE_TRUNC('month', pickup_time) AS month,
    COUNT(*) AS total_trips,
    SUM(total_usd) AS total_revenue,
    AVG(distance_miles) AS avg_distance,
    AVG(fare_usd) AS avg_fare,
    AVG(tip_usd) AS avg_tip,
    AVG(tip_usd / NULLIF(fare_usd, 0)) * 100 AS avg_tip_pct,
    SUM(passengers) AS total_passengers
FROM silver_taxi_data
GROUP BY month
ORDER BY month;
"""

try:
    df_gold = pd.read_sql(query, engine)
    print(f"📊 Aggregated {len(df_gold)} monthly rows from 'silver_taxi_data'")
    print("🔍 Preview of Gold metrics:")
    print(df_gold.head())

    df_gold.to_sql("gold_taxi_metrics", engine, if_exists="replace", index=False)
    print("✅ Gold table 'gold_taxi_metrics' created successfully.")

    # 📤 Export to CSV
    df_gold.to_csv("gold_taxi_metrics.csv", index=False)
    print("📁 Exported Gold metrics to 'gold_taxi_metrics.csv'")

except Exception as e:
    print(f"❌ Error building Gold table: {e}")
>>>>>>> 3a3e6a2827ca1347820474a39d570a74bc2ef67a
