# from extract import fetch_weather
# from transform import clean_data
# from load import save_to_db

# def run_pipeline():
#     raw = fetch_weather()
#     cleaned = clean_data(raw)
#     save_to_db(cleaned)

# if __name__ == "__main__":
#     run_pipeline()


import requests
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

# =====================
# LOGGING CONFIG
# =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

# =====================
# LOAD ENV
# =====================
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

logging.info("🚀 Starting pipeline...")

# =====================
# 1. EXTRACT
# =====================
url = "https://api.open-meteo.com/v1/forecast?latitude=49.83&longitude=18.29&hourly=temperature_2m"

logging.info("Fetching data from API...")
response = requests.get(url)
data = response.json()

logging.info("✅ Data fetched")

if response.status_code != 200:
    logging.error(f"API error: {response.status_code}")
    raise Exception("API request failed")

# =====================
# 2. INSPECT RAW DATA
# =====================
logging.info(f"Raw keys: {data.keys()}")
logging.info(f"Hourly keys: {data['hourly'].keys()}")

# =====================
# 3. TRANSFORM
# =====================
logging.info("Transforming data into DataFrame...")

df = pd.DataFrame({
    "time": data["hourly"]["time"],
    "temperature": data["hourly"]["temperature_2m"]
})

# =====================
# 4. INSPECT DATAFRAME
# =====================
logging.info(f"HEAD:\n{df.head()}")
logging.info(f"DTYPES:\n{df.dtypes}")
logging.info(f"NULLS:\n{df.isnull().sum()}")
logging.info(f"STATS:\n{df.describe()}")

# =====================
# 5. CLEAN
# =====================
logging.info("Cleaning data...")

df["time"] = pd.to_datetime(df["time"])
df = df.dropna()

logging.info(f"Rows after cleaning: {len(df)}")

# =====================
# 6. LOAD
# =====================
logging.info("Connecting to database...")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

try:
    logging.info("Saving data to database...")
    df.to_sql("weather_data", engine, if_exists="append", index=False)
    logging.info("✅ Data saved to database")
except Exception as e:
    logging.error(f"❌ Error saving data: {e}")
    raise

logging.info("🎉 Pipeline finished")