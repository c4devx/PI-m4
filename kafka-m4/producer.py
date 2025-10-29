import os, json, time, requests, sys
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "openweather_topic")
CITY = os.getenv("CITY_NAME", "Patagonia,ar")

POLL_SECS = int(os.getenv("POLL_SECS", "120"))
MAX_RUNS = int(os.getenv("MAX_RUNS", "0"))
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() in {"1","true","yes"}

def build_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all"
    )

def fetch_weather(city: str):
    if not API_KEY:
        print("[ERROR] Falta la API key de OpenWeather (OPENWEATHER_API_KEY)", file=sys.stderr)
        sys.exit(1)

    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric&lang=es"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    js = r.json()
    
    return {
        "source": "openweather",
        "city": js.get("name"),
        "lat": js["coord"]["lat"],
        "lon": js["coord"]["lon"],
        "ts": int(time.time() * 1000),
        "datetime_utc": datetime.fromtimestamp(js.get("dt"), tz=timezone.utc).isoformat().replace('+00:00', 'Z'),
        "temp_c": js["main"]["temp"],
        "humidity": js["main"]["humidity"],
        "pressure": js["main"]["pressure"],
        "wind_speed": js["wind"]["speed"]
    }

def main():
    producer = build_producer()
    print(f"[producer] broker={BROKER} topic={TOPIC} city={CITY}")

    iteration = 0
    while True:
        try:
            data = fetch_weather(CITY)
            producer.send(TOPIC, data).get(timeout=30)
            print(f"[producer] Enviado -> {data['city']}  temp_c={data['temp_c']}  {data['datetime_utc']}")
            iteration += 1
        except Exception as e:
            print(f"[producer] ERROR: {e}", file=sys.stderr)

        if RUN_ONCE or (MAX_RUNS and iteration >= MAX_RUNS):
            break
        time.sleep(POLL_SECS)

    print(f"[producer] OK --> {iteration} mensajes")

if __name__ == "__main__":
    main()
