import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

from ctes import c_silver as cs
from utils import u_silver as us

def build_spark(app_name="weather-silver-job", shuffle_parts=8):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(shuffle_parts))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.warehouse.dir", "/opt/etlt/app/spark-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    print("[Spark] master                         =", spark.sparkContext.master)
    print("[Spark] fs.s3a.impl                    =", hconf.get("fs.s3a.impl"))
    print("[Spark] fs.s3a.endpoint                =", hconf.get("fs.s3a.endpoint"))
    print("[Spark] fs.s3a.credentials.provider    =", hconf.get("fs.s3a.aws.credentials.provider"))
    print("[Spark] shuffle.partitions             =", spark.conf.get("spark.sql.shuffle.partitions"))
    return spark

def read_airbyte(sp, bronze_root, city, y, m, d):
    path = f"{bronze_root}/api_airbyte/openweather/{cs.CITY_PREFIX_OW[city]}/year={y}/month={m:02d}/day={d:02d}/"
    return sp.read.parquet(path).withColumn("src_system", F.lit("airbyte"))

def read_manual(sp, bronze_root, city, ingest_date):
    path = f"{bronze_root}/manual_upload/{cs.CITY_PREFIX_MAN[city]}/ingest_date={ingest_date}/*.json"
    df = sp.read.option("multiLine", True).json(path)

    if len(df.schema.fields) == 1 and isinstance(df.schema.fields[0].dataType, T.ArrayType):
        root = df.schema.fields[0].name
        df = df.select(F.explode(F.col(root)).alias("obj")).select("obj.*")
    return df.withColumn("src_system", F.lit("manual"))

def normalize_df(df, job_city):
    cols = set(df.columns)

    # ------------------------------
    # Metadata / Origen
    # ------------------------------
    base_meta = df.withColumn("src_system", F.coalesce(F.col("src_system"), F.lit("openweather")))

    # ------------------------------
    # Weather array
    # ------------------------------
    if "weather" in cols:
        first_weather = F.when(
            (F.col("weather").isNotNull()) & (F.size(F.col("weather")) > 0),
            F.element_at(F.col("weather"), 1)
        )
        w_main = first_weather.getField("main").cast("string")
        w_desc = first_weather.getField("description").cast("string")
    else:
        w_main = F.lit(None).cast("string")
        w_desc = F.lit(None).cast("string")

    # ------------------------------
    # Precipitaciones y UV
    # ------------------------------
    rain_1h_raw = us.extract_hourly_precipitation(df, "rain") if "rain" in df.columns else F.lit(None).cast("double")
    snow_1h_raw = us.extract_hourly_precipitation(df, "snow") if "snow" in df.columns else F.lit(None).cast("double")
    uv_index_expr = F.col("uv_index").cast("double") if "uv_index" in cols else F.lit(None).cast("double")

    # ------------------------------
    # Tiempo base UTC y local
    # ------------------------------
    dt_sec = F.col("dt").cast("long")
    tz_sec = F.coalesce(F.col("timezone").cast("int"), F.lit(0))

    # ------------------------------
    # Normalización final con derivadas y bins
    # ------------------------------
    silver = (
        base_meta

        .withColumn("lat", F.col("coord.lat").cast("double") if "coord" in base_meta.columns else F.col("lat").cast("double"))
        .withColumn("lon", F.col("coord.lon").cast("double") if "coord" in base_meta.columns else F.col("lon").cast("double"))
        .withColumn("city", F.lit(job_city))

        # Tiempo
        .withColumn("event_ts",   F.to_timestamp(F.from_unixtime(dt_sec)))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("hour",       F.hour("event_ts"))
        .withColumn("timezone_off", tz_sec)
        .withColumn("local_ts",     F.to_timestamp(F.from_unixtime(dt_sec + tz_sec)))
        .withColumn("local_hour",   F.hour("local_ts"))
        .withColumn("is_daylight",  ((F.col("local_hour") >= 7) & (F.col("local_hour") <= 19)).cast("int"))

        # Variables meteorológicas
        .withColumn("temp",        F.col("main.temp").cast("double"))
        .withColumn("feels_like",  F.col("main.feels_like").cast("double"))
        .withColumn("humidity",    F.col("main.humidity").cast("int"))
        .withColumn("pressure",    F.col("main.pressure").cast("int"))
        .withColumn("wind_speed",  F.col("wind.speed").cast("double"))
        .withColumn("wind_deg",    F.col("wind.deg").cast("int"))
        .withColumn("clouds_pct",  F.col("clouds.all").cast("int"))
        .withColumn("rain_1h",     F.coalesce(rain_1h_raw, F.lit(0.0)))
        .withColumn("snow_1h",     F.coalesce(snow_1h_raw, F.lit(0.0)))
        .withColumn("precip_1h",   (F.col("rain_1h") + F.col("snow_1h")).cast("double"))
        .withColumn("weather_main", w_main)
        .withColumn("weather_desc", w_desc)
        .withColumn("uv_index",     uv_index_expr)

        # Derivadas solares y físicas
        .withColumn("solar_uv_proxy", F.col("uv_index"))
        .withColumn("solar_cloud_proxy", F.when(F.col("uv_index").isNull() & (F.col("is_daylight") == 1),
           1.0 - (F.col("clouds_pct") / 100.0)).otherwise(F.lit(0.0))
        )
        .withColumn("solar_potential_raw", F.coalesce(F.col("solar_uv_proxy"), F.col("solar_cloud_proxy")))
        .withColumn("air_density",
            F.when(
                F.col("pressure").isNotNull() & F.col("temp").isNotNull(),
                (
                    (F.col("pressure") * F.lit(100.0) * F.lit(0.0289644)) /
                    (
                        (F.lit(8.314462618)) *
                        (F.col("temp") + F.lit(273.15)) *
                        (F.lit(1.0) - ((F.col("pressure") * 100) / (F.col("temp") + 273.15)) * F.lit(1e-7))
                    )
                )
            ).otherwise(F.lit(None)).cast("double")
        )
        .withColumn("wind_power_wm2",
            F.when(
                F.col("wind_speed").isNotNull() & (F.col("wind_speed") > 0),
                0.5 * F.col("air_density") * F.pow(F.col("wind_speed"), 3) * 0.4 * 0.9
            ).otherwise(F.lit(0.0))
        )

        # Bins categóricos
        .withColumn("cloud_bin",    us.categorize_numeric(F.col("clouds_pct"), [20,40,60,80]))
        .withColumn("wind_bin",     us.categorize_numeric(F.col("wind_speed"), [2.5,5.0,7.5,10.0]))
        .withColumn("humidity_bin", us.categorize_numeric(F.col("humidity"), [20,40,60,80]))

        # Auditoría
        .withColumn("etl_run_ts", F.current_timestamp())

        # Limpieza
        .drop("main","wind","clouds","weather","rain","snow","coord",
              "city_name","name","dt_iso","base","sys","visibility","id","cod","timezone")
        .dropDuplicates(["city","event_ts"])

        # Particiones
        .withColumn("event_year",  F.year("event_date").cast("int"))
        .withColumn("event_month", F.month("event_date").cast("int"))
        .withColumn("event_day",   F.dayofmonth("event_date").cast("int"))
    )

    return silver.select([c for c in cs.DF_COLS if c in silver.columns])

def read_silver_partition(sp, silver_root, city, y, m, d):
    path = f"{silver_root}/weather/city={city}/event_year={y}/event_month={m}/event_day={d}/"
    try:
        return sp.read.parquet(path)
    except Exception:
        return None

def write_silver(df_lite, silver_root):
    required = ["event_ts"] + cs.PARTITION_COLS
    before = df_lite.count()
    df_clean = df_lite.na.drop(subset=required)
    after = df_clean.count()
    dropped = before - after
    print(f"[DF_QUALITY] # originales: {before} | # sin particiones nulas: {after} | # descartados: {dropped}")

    (df_clean
     .repartition(*cs.PARTITION_COLS)
     .write.mode("overwrite")
     .partitionBy(*cs.PARTITION_COLS)
     .option("compression","snappy")
     .parquet(f"{silver_root}/weather"))

def run_historical_mode(sp, bronze_root, silver_root, city, ingest_date):
    df = read_manual(sp, bronze_root, city, ingest_date)
    write_silver(normalize_df(df, city), silver_root)

def run_day_mode(sp, bronze_root, silver_root, city, y, m, d):
    new_df = normalize_df(read_airbyte(sp, bronze_root, city, y, m, d), city)
    old_df = read_silver_partition(sp, silver_root, city, y, m, d)
    merged = old_df.unionByName(new_df, allowMissingColumns=True).dropDuplicates(["city","event_ts"]) if old_df is not None else new_df
    write_silver(merged, silver_root)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETLT M4: BRONZE → SILVER")
    parser.add_argument("--mode", choices=["historical_mode","day_mode"], required=True)
    parser.add_argument("--bronze", default="s3a://dept01-m4-bronze")
    parser.add_argument("--silver", default="s3a://dept01-m4-silver")
    parser.add_argument("--city", choices=list(cs.CITY_PREFIX_MAN.keys()), required=True)
    parser.add_argument("--date", help="YYYY-MM-DD (--mode day_mode)", default=None)
    parser.add_argument("--ingest-date", help="YYYY-MM-DD (--mode historical_mode)", default=None)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    args = parser.parse_args()

    spark = build_spark(shuffle_parts=args.shuffle_partitions)

    match args.mode:
        case "historical_mode":
            if not args.ingest_date:
                raise SystemExit("--ingest-date es REQUERIDO para historical_mode")
            run_historical_mode(spark, args.bronze, args.silver, args.city, args.ingest_date)

        case "day_mode":
            if not args.date:
                raise SystemExit("--date es REQUERIDO para day_mode")
            y, m, d = map(int, args.date.split("-"))
            run_day_mode(spark, args.bronze, args.silver, args.city, y, m, d)

        case _:
            raise SystemExit(f"Modo no reconocido: {args.mode}")


    print(f"[weather_silver_job] finalizado OK : mode={args.mode} city={args.city}")