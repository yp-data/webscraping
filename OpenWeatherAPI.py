import requests
import psycopg2
import time
import sys
import subprocess  # ✅ Import for running external scripts
from datetime import datetime

# 🔹 PostgreSQL Database Config
DB_NAME = "weather_db"
DB_USER = # Enter your username here
DB_PASSWORD = # Enter your password here
DB_HOST = "localhost"
DB_PORT = "5432"

# 🔹 OpenWeather API Key
API_KEY = "81f6d002f176ade77d7d9224449934b9" 

# 🔹 List of Cities
CITIES = ["New York", "London", "Paris", "Tokyo", "Sydney"]

# 🔹 API URLs
FORECAST_API_URL = "https://api.openweathermap.org/data/2.5/forecast"
CURRENT_API_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather_forecast(city):
    """Fetch 5-day weather forecast for a city"""
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    response = requests.get(FORECAST_API_URL, params=params)
    data = response.json()

    if "list" not in data:
        print(f"❌ Error fetching forecast for {city}: {data}")
        return []

    forecasts = []
    for entry in data["list"]:
        timestamp = datetime.utcfromtimestamp(entry["dt"])
        record_date = timestamp.date()

        temp = round(entry["main"]["temp"], 2)
        humidity = entry["main"]["humidity"]
        weather_desc = entry["weather"][0]["description"]

        forecasts.append(("forecast", city, str(record_date), timestamp, temp, humidity, weather_desc))

    return forecasts


def fetch_current_weather(city):
    """Fetch real-time current weather for a city"""
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    response = requests.get(CURRENT_API_URL, params=params)
    data = response.json()

    if "main" not in data:
        print(f"❌ Error fetching current weather for {city}: {data}")
        return None

    timestamp = datetime.utcfromtimestamp(data["dt"])
    record_date = timestamp.date()

    temp = round(data["main"]["temp"], 2)
    humidity = data["main"]["humidity"]
    weather_desc = data["weather"][0]["description"]

    return ("current", city, str(record_date), timestamp, temp, humidity, weather_desc)


def store_weather_data(weather_records):
    """Store weather data (forecast + current) in PostgreSQL and run weather_analysis.py"""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()

        sql = """
        INSERT INTO weather_data (source, city, date, timestamp, temperature, humidity, weather_description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city, timestamp) DO NOTHING;
        """

        for record in weather_records:
            print(f"📝 Inserting: {record}")  # Debugging line
            cur.execute(sql, record)

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Stored {len(weather_records)} weather records in PostgreSQL!")

        # ✅ Run `weather_analysis.py` after inserting data
        print("📊 Running weather analysis script...")
        subprocess.run(["python", "C:\\Users\\ri\\Documents\\Data app\\Work\\weather_analysis.py"])

    except Exception as e:
        print(f"❌ Error storing data: {e}")


def run_weather_script():
    """Fetch and store weather data for multiple cities"""
    all_weather_data = []

    for city in CITIES:
        print(f"🔍 Fetching forecast for {city}...")
        forecasts = fetch_weather_forecast(city)
        if forecasts:
            all_weather_data.extend(forecasts)

        print(f"🔍 Fetching current weather for {city}...")
        current_weather = fetch_current_weather(city)
        if current_weather:
            all_weather_data.append(current_weather)

    if all_weather_data:
        store_weather_data(all_weather_data)

    print("🏁 Finished fetching and storing weather data!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "manual":
        print("🚀 Running **MANUAL MODE** (One-time execution)")
        run_weather_script()
    else:
        print("⏳ Running **SCHEDULER MODE** (Every 5 minutes)")
        while True:
            run_weather_script()
            time.sleep(300)  # 300 seconds = 5 minutes
