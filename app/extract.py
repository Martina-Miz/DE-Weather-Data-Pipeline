import requests
import logging

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=49.83&longitude=18.29&hourly=temperature_2m"
    
    logging.info("Fetching data from API...")

    try:
        response = requests.get(url)
        response.raise_for_status()  # vyhodí error pro 4xx/5xx

        data = response.json()

        logging.info("✅ Data fetched")
        logging.info(f"Raw keys: {data.keys()}")
        logging.info(f"Hourly keys: {data['hourly'].keys()}")

        return data

    except Exception as e:
        logging.error(f"❌ Error fetching data: {e}")
        return None