CITY_PREFIX_MAN = {
    "Patagonia": "city_patagonia",
    "Riohacha":  "city_riohacha",
}

CITY_PREFIX_OW = {
    "Patagonia": "API2.5OWPatagonia",
    "Riohacha":  "API2.5OWRiohacha",
}

DF_COLS = [
    "city", "event_ts", "event_date", "hour",
    "timezone_off", "local_ts", "local_hour", "is_daylight",
    "event_year", "event_month", "event_day",
    "lat", "lon",
    "temp", "feels_like", "humidity", "pressure",
    "wind_speed", "wind_deg", "clouds_pct",
    "rain_1h", "snow_1h", "precip_1h",
    "weather_main", "weather_desc",
    "uv_index",
    "solar_uv_proxy", "solar_cloud_proxy", "solar_potential_raw",
    "air_density", "wind_power_wm2",
    "cloud_bin", "wind_bin", "humidity_bin",
    "src_system", "etl_run_ts",
]

PARTITION_COLS = ["city","event_year","event_month","event_day"]