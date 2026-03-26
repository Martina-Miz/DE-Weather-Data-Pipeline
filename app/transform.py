import pandas as pd
import logging

def clean_data(raw_data):
    if raw_data is None:
        logging.error("❌ No raw data received for transformation")
        return None

    logging.info("Transforming data into DataFrame...")

    df = pd.DataFrame({
        "time": raw_data["hourly"]["time"],
        "temperature": raw_data["hourly"]["temperature_2m"]
    })

    logging.info(f"HEAD:\n{df.head()}")
    logging.info(f"DTYPES:\n{df.dtypes}")
    logging.info(f"NULLS:\n{df.isnull().sum()}")

    logging.info("Cleaning data...")

    df["time"] = pd.to_datetime(df["time"])
    df = df.dropna()

    logging.info(f"Rows after cleaning: {len(df)}")

    return df