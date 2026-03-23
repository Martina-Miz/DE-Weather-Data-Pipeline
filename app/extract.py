import requests

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=49.83&longitude=18.29&hourly=temperature_2m"
    
    # Fetch weather data from the API
    try: #
        response = requests.get(url) 
        response.raise_for_status() # Check if the request was successful
        return response.json()
    except Exception as e:  
        print(f"Error fetching data: {e}")
        return None