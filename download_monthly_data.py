<<<<<<< HEAD
import os
import requests

# 📁 Create folder to store data
DOWNLOAD_DIR = "data"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# 🌐 Base URL and months
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEAR = 2024
MONTHS = [f"{month:02d}" for month in range(1, 13)]

def download_monthly_data():
    files_downloaded = 0

    for month in MONTHS:
        file_name = f"yellow_tripdata_{YEAR}-{month}.parquet"
        url = f"{BASE_URL}/{file_name}"
        local_path = os.path.join(DOWNLOAD_DIR, file_name)

        if os.path.exists(local_path):
            print(f"✅ {file_name} already downloaded. Skipping.")
            continue

        print(f"⬇️ Downloading {file_name}...")
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                with open(local_path, "wb") as f:
                    f.write(response.content)
                print(f"📁 Saved to {local_path}")
                files_downloaded += 1
            else:
                print(f"❌ Failed to download {file_name}. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Error downloading {file_name}: {e}")

    print(f"\n🎉 Finished downloading {files_downloaded} new file(s) to '{DOWNLOAD_DIR}'.")

if __name__ == "__main__":
    download_monthly_data()
=======
import os
import requests

# 📁 Create folder to store data
DOWNLOAD_DIR = "data"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# 🌐 Base URL and months
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEAR = 2024
MONTHS = [f"{month:02d}" for month in range(1, 13)]

def download_monthly_data():
    files_downloaded = 0

    for month in MONTHS:
        file_name = f"yellow_tripdata_{YEAR}-{month}.parquet"
        url = f"{BASE_URL}/{file_name}"
        local_path = os.path.join(DOWNLOAD_DIR, file_name)

        if os.path.exists(local_path):
            print(f"✅ {file_name} already downloaded. Skipping.")
            continue

        print(f"⬇️ Downloading {file_name}...")
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                with open(local_path, "wb") as f:
                    f.write(response.content)
                print(f"📁 Saved to {local_path}")
                files_downloaded += 1
            else:
                print(f"❌ Failed to download {file_name}. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Error downloading {file_name}: {e}")

    print(f"\n🎉 Finished downloading {files_downloaded} new file(s) to '{DOWNLOAD_DIR}'.")

if __name__ == "__main__":
    download_monthly_data()
>>>>>>> 3a3e6a2827ca1347820474a39d570a74bc2ef67a
