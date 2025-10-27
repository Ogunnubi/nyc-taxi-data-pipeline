<<<<<<< HEAD
import pandas as pd

data = {
    'passenger_count': [1, 2, 1],
    'fare_amount': [5.0, 12.5, 7.0],
    'tip_amount': [1.0, 2.0, 1.5],
    'tpep_pickup_datetime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
}

df = pd.DataFrame(data)
df.to_parquet('data/sample_taxi_data.parquet')

print("✅ Sample Parquet file created in 'data/' folder.")
=======
import pandas as pd

data = {
    'passenger_count': [1, 2, 1],
    'fare_amount': [5.0, 12.5, 7.0],
    'tip_amount': [1.0, 2.0, 1.5],
    'tpep_pickup_datetime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
}

df = pd.DataFrame(data)
df.to_parquet('data/sample_taxi_data.parquet')

print("✅ Sample Parquet file created in 'data/' folder.")
>>>>>>> 3a3e6a2827ca1347820474a39d570a74bc2ef67a
