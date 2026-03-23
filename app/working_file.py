import requests
import pandas as pd

#stazeni dat z API
url = "https://api.open-meteo.com/v1/forecast?latitude=49.19&longitude=16.61&hourly=temperature_2m"

response = requests.get(url)
data = response.json()

#kontrola struktury dat
print(data.keys())
print(data["hourly"].keys())

#vytvoreni dataframe
df = pd.DataFrame({
    "time": data["hourly"]["time"],
    "temperature": data["hourly"]["temperature_2m"]
})

print("\nData z Brna:")
print(df.head(10))
print(df.dtypes)
print(df.isnull().sum())
print(df.describe())

df["time"] = pd.to_datetime(df["time"]) # Convert time to datetime format
df = df.dropna() #` Remove rows with missing values

print(df.head())
print(df.dtypes)