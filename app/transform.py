import pandas as pd

def clean_data(raw_data):
    if raw_data is None:
        return None

    df = pd.DataFrame({
        "time": raw_data["hourly"]["time"],
        "temperature": raw_data["hourly"]["temperature_2m"]
    })

    df["time"] = pd.to_datetime(df["time"])
    df = df.dropna()

    return df