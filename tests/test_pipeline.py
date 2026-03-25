import requests

def test_api_response():
    url = "https://api.open-meteo.com/v1/forecast?latitude=49.83&longitude=18.29&hourly=temperature_2m"
    response = requests.get(url)

    assert response.status_code == 200

    data = response.json()
    assert "hourly" in data

import pandas as pd
import requests

def test_dataframe_creation():
    url = "https://api.open-meteo.com/v1/forecast?latitude=49.83&longitude=18.29&hourly=temperature_2m"
    data = requests.get(url).json()

    df = pd.DataFrame({
        "time": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"]
    })

    assert not df.empty
    assert "time" in df.columns
    assert "temperature" in df.columns

def test_cleaning():
    import pandas as pd

    df = pd.DataFrame({
        "time": ["2024-01-01", None],
        "temperature": [10, None]
    })

    df["time"] = pd.to_datetime(df["time"])
    df = df.dropna()

    assert len(df) == 1

from sqlalchemy import create_engine
import os

def test_db_connection():
    DB_USER = os.getenv("DB_USER", "user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "weather")

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    connection = engine.connect()
    assert connection is not None
    connection.close()