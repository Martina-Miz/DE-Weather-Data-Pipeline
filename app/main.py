import logging
from extract import fetch_weather
from transform import clean_data
from load import save_to_db

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

def run_pipeline():
    logging.info("🚀 Starting pipeline...")

    # 1. EXTRACT
    raw = fetch_weather()
    if raw is None:
        logging.error("❌ Pipeline stopped - extract failed")
        return

    # 2. TRANSFORM
    cleaned = clean_data(raw)
    if cleaned is None:
        logging.error("❌ Pipeline stopped - transform failed")
        return

    # 3. LOAD
    save_to_db(cleaned)

    logging.info("🎉 Pipeline finished")


if __name__ == "__main__":
    run_pipeline()