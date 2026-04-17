import requests 

def extract():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 51.5074,
        "longitude": -0.1278,
        "hourly": "temperature_2m,wind_speed_10m,precipitation",
        "past_days": 7,
        "timezone": "Europe/London"
    }
    response = requests.get(url, params=params)
    data = response.json()
  
    print(data['hourly'])
    return data

if __name__ == "__main__":
    extract()