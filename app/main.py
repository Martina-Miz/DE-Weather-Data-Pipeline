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

# load env variables
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

print("🚀 Starting pipeline...")

# =====================
# 1. EXTRACT
# =====================
url = "https://api.open-meteo.com/v1/forecast?latitude=49.83&longitude=18.29&hourly=temperature_2m"

response = requests.get(url)
data = response.json()

print("✅ Data fetched")

# =====================
# 2. INSPECT RAW DATA
# =====================
print(data.keys())
print(data["hourly"].keys())

# =====================
# 3. TRANSFORM
# =====================
df = pd.DataFrame({
    "time": data["hourly"]["time"],
    "temperature": data["hourly"]["temperature_2m"]
})

# =====================
# 4. INSPECT DATAFRAME
# =====================
print("\nHEAD:\n", df.head())
print("\nDTYPES:\n", df.dtypes)
print("\nNULLS:\n", df.isnull().sum())
print("\nSTATS:\n", df.describe())

# =====================
# 5. CLEAN
# =====================
df["time"] = pd.to_datetime(df["time"])
df = df.dropna()

print("Rows after cleaning:", len(df))

# =====================
# 6. LOAD
# =====================
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

df.to_sql("weather_data", engine, if_exists="append", index=False)

print("✅ Data saved to database")
print("🎉 Pipeline finished")