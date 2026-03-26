import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def save_to_db(df):
    logging.info("Connecting to database...")

    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")

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