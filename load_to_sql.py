<<<<<<< HEAD
import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

# 🔐 Load environment variables from .env file
load_dotenv()

# 📁 Directory containing Parquet files
DATA_DIR = "data"
TABLE_NAME = "raw_taxi_data"

# 🔐 PostgreSQL connection settings from .env
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME")
}

# 🔐 Encode password safely for URL
encoded_password = quote_plus(DB_CONFIG["password"])

# 🔌 Create SQLAlchemy engine
engine_url = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)
print(f"🔌 Connecting to PostgreSQL with URL: {engine_url}")
engine = create_engine(engine_url, connect_args={"host": DB_CONFIG["host"]})
print("✅ SQLAlchemy engine created successfully.")

# 🚛 Load Parquet files into PostgreSQL
def load_parquet_to_postgres():
    parquet_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")]
    print(f"🔍 Found {len(parquet_files)} Parquet files in '{DATA_DIR}'")
    print(f"📁 Files: {parquet_files}")

    files_loaded = 0

    for filename in parquet_files:
        file_path = os.path.join(DATA_DIR, filename)
        print(f"\n📦 Loading {filename} into table '{TABLE_NAME}'...")

        df = pd.read_parquet(file_path)
        df.columns = [col.lower() for col in df.columns]

        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        print(f"✅ Successfully loaded {len(df)} rows from {filename}")
        files_loaded += 1

    print(f"\n🎉 Finished loading {files_loaded} file(s) into '{TABLE_NAME}'.")

# ✅ Entry point
if __name__ == "__main__":
    load_parquet_to_postgres()
=======
import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

# 🔐 Load environment variables from .env file
load_dotenv()

# 📁 Directory containing Parquet files
DATA_DIR = "data"
TABLE_NAME = "raw_taxi_data"

# 🔐 PostgreSQL connection settings from .env
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME")
}

# 🔐 Encode password safely for URL
encoded_password = quote_plus(DB_CONFIG["password"])

# 🔌 Create SQLAlchemy engine
engine_url = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)
print(f"🔌 Connecting to PostgreSQL with URL: {engine_url}")
engine = create_engine(engine_url, connect_args={"host": DB_CONFIG["host"]})
print("✅ SQLAlchemy engine created successfully.")

# 🚛 Load Parquet files into PostgreSQL
def load_parquet_to_postgres():
    parquet_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")]
    print(f"🔍 Found {len(parquet_files)} Parquet files in '{DATA_DIR}'")
    print(f"📁 Files: {parquet_files}")

    files_loaded = 0

    for filename in parquet_files:
        file_path = os.path.join(DATA_DIR, filename)
        print(f"\n📦 Loading {filename} into table '{TABLE_NAME}'...")

        df = pd.read_parquet(file_path)
        df.columns = [col.lower() for col in df.columns]

        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        print(f"✅ Successfully loaded {len(df)} rows from {filename}")
        files_loaded += 1

    print(f"\n🎉 Finished loading {files_loaded} file(s) into '{TABLE_NAME}'.")

# ✅ Entry point
if __name__ == "__main__":
    load_parquet_to_postgres()
>>>>>>> 3a3e6a2827ca1347820474a39d570a74bc2ef67a
