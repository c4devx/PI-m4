import argparse
from pyspark.sql import SparkSession, functions as F, Window as W

from ctes import c_gold as cg
from utils import u_gold as ug

def build_spark(app_name="weather-gold-job", shuffle_parts=16):
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
    return spark

def write_parquet(df, path, parts=None, max_records_per_file=None):
    w = df.write.mode("overwrite").option("compression","snappy")
    if parts: w = w.partitionBy(*parts)
    if max_records_per_file: w = w.option("maxRecordsPerFile", str(max_records_per_file))
    w.parquet(path)

def main(args):
    spark = build_spark(shuffle_parts=args.shuffle_partitions)

    silver = spark.read.parquet(cg.BASE_SILVER_BUCKET)
    silver = ug.filter_and_cache(silver, args.years_keep, args.months_keep, args.cities)

    dim_date = (
        silver.select(F.to_date("event_date").alias("date"))
              .dropna().distinct()
              .withColumn("date_key", F.date_format("date","yyyyMMdd").cast("int"))
              .withColumn("year",  F.year("date"))
              .withColumn("month", F.month("date"))
              .withColumn("day",   F.dayofmonth("date"))
              .select("date_key","date","year","month","day")
    )

    dim_city = (
        silver.select(F.col("city").alias("city_name"),"lat","lon")
              .dropDuplicates(["city_name"])
              .withColumn("city_key", ug.hash_key(F.col("city_name")))
              .select("city_key","city_name","lat","lon")
    )

    dim_weather_condition = (
        silver.select("weather_main","weather_desc")
              .fillna({"weather_main":"Unknown","weather_desc":"Unknown"})
              .dropDuplicates()
              .withColumn("weather_key", ug.hash_key(F.concat_ws("_", F.col("weather_main"), F.col("weather_desc"))))
              .select("weather_key","weather_main","weather_desc")
    )

    write_parquet(dim_date, f"{cg.BASE_GOLD_BUCKET}/dim_date", max_records_per_file=args.max_records_per_file)
    write_parquet(dim_city, f"{cg.BASE_GOLD_BUCKET}/dim_city", max_records_per_file=args.max_records_per_file)
    write_parquet(dim_weather_condition, f"{cg.BASE_GOLD_BUCKET}/dim_weather_condition",
                  max_records_per_file=args.max_records_per_file)

    d_date = ug.prepare_dim(dim_date, ["date_key", "date", "year", "month", "day"])
    d_city = ug.prepare_dim(dim_city, ["city_key", "city_name"])
    d_wc   = ug.prepare_dim(dim_weather_condition, ["weather_key", "weather_main", "weather_desc"])

    coreFeats = silver.select(
        "city","event_date","event_year","event_month","local_hour",
        F.col("is_daylight").cast("int").alias("is_daylight"),
        "solar_potential_raw","wind_power_wm2",
        "wind_speed","wind_deg","temp","feels_like",
        "precip_1h","humidity","clouds_pct",
        "weather_main","weather_desc"
    )

    pa_solar = ug.compute_percentiles(coreFeats, "solar_potential_raw")
    pa_wind  = ug.compute_percentiles(coreFeats, "wind_power_wm2")

    # # pa_solar  = (coreFeats.groupBy("city","event_month")
    #                 # .agg(F.expr("percentile_approx(solar_potential_raw, array(0.05,0.95), 250) as pct")))
    # # pa_wind   = (coreFeats.groupBy("city","event_month")
    #                 # .agg(F.expr("percentile_approx(wind_power_wm2, array(0.05,0.95), 250) as pct")))

    pct_city = (
        pa_solar.alias("sa")
            .join(pa_wind.alias("wa"), ["city","event_month"], "full_outer")
            .select(
            F.coalesce(F.col("sa.city"), F.col("wa.city")).alias("city"),
            F.col("event_month"),
            F.col("sa.p5").alias("solar_p5_city"),
            F.col("sa.p95").alias("solar_p95_city"),
            F.col("wa.p5").alias("wind_p5_city"),
            F.col("wa.p95").alias("wind_p95_city")
        )
        .withColumn("solar_p95_city", F.greatest(F.col("solar_p95_city"), F.col("solar_p5_city") + F.lit(1e-6)))
        .withColumn("wind_p95_city",  F.greatest(F.col("wind_p95_city"),  F.col("wind_p5_city")  + F.lit(1e-6)))
    )

    hourly_n = (
        coreFeats.join(pct_city, ["city","event_month"], "left")
            .withColumn("solar_p5",  F.coalesce(F.col("solar_p5_city"), F.lit(0.0)))
            .withColumn("solar_p95", F.coalesce(F.col("solar_p95_city"), F.lit(1.0)))
            .withColumn("wind_p5",   F.coalesce(F.col("wind_p5_city"), F.lit(0.0)))
            .withColumn("wind_p95",  F.coalesce(F.col("wind_p95_city"), F.lit(1.0)))
            .withColumn(
                "solar_norm",
                F.when(F.col("is_daylight")==0, 0.0)
                 .otherwise(ug.normalize_with_guard(F.col("solar_potential_raw"), F.col("solar_p5"), F.col("solar_p95")))
            )
            .withColumn(
                "wind_norm",
                ug.bound_to_unit_interval(ug.normalize_with_guard(F.col("wind_power_wm2"), F.col("wind_p5"), F.col("wind_p95")))
            )
            .withColumn("solar_norm", ug.bound_to_unit_interval(F.when(F.isnan("solar_norm") | F.col("solar_norm").isNull(), 0.0)
                                               .otherwise(F.col("solar_norm"))))
            .withColumn("wind_norm",  ug.bound_to_unit_interval(F.when(F.isnan("wind_norm")  | F.col("wind_norm").isNull(),  0.0)
                                               .otherwise(F.col("wind_norm"))))
            .withColumn("rps_hour",
                F.when(F.col("is_daylight")==1, 0.6*F.col("solar_norm")+0.4*F.col("wind_norm"))
                 .otherwise(F.col("wind_norm"))
            )
    )

    fact_weather_hourly = (
        hourly_n
        .withColumn("event_date", F.to_date("event_date"))
        .withColumn("date_key", F.date_format("event_date","yyyyMMdd").cast("int"))
        .join(d_city, hourly_n.city == d_city.city_name, "left")
        .join(d_wc,   ["weather_main","weather_desc"], "left")
        .select(
            "date_key","city_key","weather_key",
            "event_date","event_year","event_month","local_hour",
            F.col("is_daylight").cast("int").alias("is_daylight"),
            "solar_norm","wind_norm","rps_hour",
            "wind_power_wm2","wind_speed","clouds_pct",
            "precip_1h","humidity","temp","feels_like"
        )
    )
    write_parquet(
        fact_weather_hourly,
        f"{cg.BASE_GOLD_BUCKET}/fact_weather_hourly",
        parts=["city_key","event_year","event_month","event_date"],
        max_records_per_file=args.max_records_per_file
    )

    fact_weather_daily = (
        fact_weather_hourly.groupBy("city_key","date_key","event_year","event_month","event_date")
        .agg(
            F.count("*").alias("hours_observed"),
            F.mean(F.when(F.col("is_daylight")==1, F.col("solar_norm"))).alias("solar_idx_day"),
            F.mean("wind_norm").alias("wind_idx_day"),
            F.mean("rps_hour").alias("rps_day"),
            F.sum("precip_1h").alias("precip_1d_sum"),
            F.sum(F.when(F.col("is_daylight")==1,1).otherwise(0)).alias("hours_daylight"),
            F.mean("clouds_pct").alias("avg_clouds"),
            F.mean("humidity").alias("avg_humidity"),
            F.max("temp").alias("max_temp"),
            F.min("temp").alias("min_temp"),
            F.mean(F.when(F.col("solar_norm")>0.6,1).otherwise(0)).alias("pct_sunny_hours"),
            F.mean(F.when(F.col("wind_norm")>0.6,1).otherwise(0)).alias("pct_windy_hours")
        )
    )

    write_parquet(
        fact_weather_daily,
        f"{cg.BASE_GOLD_BUCKET}/fact_weather_daily",
        parts=["city_key","event_year","event_month"],
        max_records_per_file=args.max_records_per_file
    )

    fact_weather_hourly = (
        spark.read
        .option("basePath", f"{cg.BASE_GOLD_BUCKET}/fact_weather_hourly")
        .parquet(f"{cg.BASE_GOLD_BUCKET}/fact_weather_hourly")
    )
    fact_weather_daily = (
        spark.read
        .option("basePath", f"{cg.BASE_GOLD_BUCKET}/fact_weather_daily")
        .parquet(f"{cg.BASE_GOLD_BUCKET}/fact_weather_daily")
    )
    dim_date            = spark.read.parquet(f"{cg.BASE_GOLD_BUCKET}/dim_date")
    dim_city            = spark.read.parquet(f"{cg.BASE_GOLD_BUCKET}/dim_city")
    dim_weather_condition = spark.read.parquet(f"{cg.BASE_GOLD_BUCKET}/dim_weather_condition")

    fact_weather_hourly = fact_weather_hourly.cache()
    fact_weather_daily  = fact_weather_daily.cache()
    dim_date            = dim_date.cache()
    dim_city            = dim_city.cache()
    dim_weather_condition = dim_weather_condition.cache()

    d_city  = F.broadcast(dim_city.select("city_key","city_name"))
    d_date  = F.broadcast(dim_date.select("date_key","date","year","month","day"))
    d_wc    = F.broadcast(dim_weather_condition.select("weather_key","weather_main","weather_desc"))

    dim_date_b  = d_date
    dim_city_b  = d_city
    dim_wx_b    = d_wc

    weather_hourly_enriched  = (fact_weather_hourly
         .join(dim_city_b,"city_key","left")
         .join(dim_date_b,"date_key","left"))
    weather_daily_enriched  = (fact_weather_daily
         .join(dim_city_b,"city_key","left")
         .join(dim_date_b,"date_key","left"))

    ym = dim_date_b.select("year","month").dropDuplicates()
    ym = ym.withColumn("first_day", F.to_date(F.concat_ws('-', F.col('year'), F.col('month'), F.lit(1))))
    days_in_month = F.broadcast(ym.select("year","month", F.dayofmonth(F.last_day("first_day")).alias("days_in_month")))

    bqs1 = (
               weather_hourly_enriched .where("is_daylight=1")
                .groupBy("city_name", "year", "month", "local_hour")
                .agg(
                   F.mean("solar_norm").alias("solar_idx_mean"),
                   F.mean("clouds_pct").alias("avg_clouds"),
                   F.mean("precip_1h").alias("precip_1h_mean"),
                   F.countDistinct("date_key").alias("obs_days")  # días con datos
                )
                .orderBy("city_name", "year", "month", "local_hour")
        )

    write_parquet(bqs1, f"{cg.BASE_GOLD_BUCKET}/bqs/solar_hr_by_month",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)


    bqs2 = (
            weather_hourly_enriched .groupBy("city_name", "year", "month", "local_hour")
                .agg(
                    F.mean("wind_norm").alias("wind_idx_mean"),
                    F.mean("wind_power_wm2").alias("wind_power_wm2_mean")
                )
            .orderBy("city_name", "year", "month", "local_hour")
        )
        
    write_parquet(bqs2, f"{cg.BASE_GOLD_BUCKET}/bqs/wind_hr_by_month",
                  parts=["city_name", "year", "month"], max_records_per_file=args.max_records_per_file)

    bqs3 = (
            weather_hourly_enriched .join(d_wc.select("weather_key","weather_main"), ["weather_key"], "left")
                .groupBy("city_name","year","month","weather_main")
                .agg(
                    F.avg("rps_hour").alias("rps_mean")
                )
        )
    
    baseline_all = weather_hourly_enriched .groupBy("city_name","year","month").agg(F.avg("rps_hour").alias("rps_baseline"))
    bqs3 = bqs3.join(F.broadcast(baseline_all), ["city_name","year","month"], "left")
    bqs3 = bqs3.withColumn(
            "rps_delta_pct",
            F.when(F.col("rps_baseline").isNull() | (F.abs(F.col("rps_baseline")) < 1e-6), None)
             .otherwise(100.0 * (F.col("rps_mean") - F.col("rps_baseline")) / F.col("rps_baseline"))
        )
    bqs3 = bqs3.select(
            "city_name","year","month","weather_main",
            "rps_mean","rps_baseline","rps_delta_pct"
        )

    write_parquet(
            bqs3, f"{cg.BASE_GOLD_BUCKET}/bqs/rps_var",
            parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file
        )


    compare = F.to_date(F.lit(args.compare_date))
    today = (weather_daily_enriched .where(F.col("date") == compare)
                   .select("city_name","date","rps_day")
                   .withColumnRenamed("rps_day","rps_today"))
    ly_date = F.add_months(compare, -12)
    ly = (weather_daily_enriched .where(F.col("date") == ly_date)
                .select("city_name", F.col("rps_day").alias("rps_last_year")))
    bqs4 = (today.join(ly, "city_name", "left")
                .withColumn("diff_abs", F.col("rps_today") - F.col("rps_last_year"))
                .withColumn(
                    "diff_pct",
                    F.when(F.abs(F.col("rps_last_year")) < 1e-6, None)  
                     .otherwise(100.0 * F.col("diff_abs") / F.col("rps_last_year"))
                ))
    write_parquet(
            bqs4, f"{cg.BASE_GOLD_BUCKET}/bqs/day_vs_last_year_day/date={args.compare_date}",
            parts=["city_name"], max_records_per_file=args.max_records_per_file
        )

    wdesc = W.partitionBy("city_name","year","month").orderBy(F.col("rps_day").desc())
    wasc  = W.partitionBy("city_name","year","month").orderBy(F.col("rps_day").asc())
    top  = (weather_daily_enriched .withColumn("rk", F.row_number().over(wdesc)).where(F.col("rk" )<=args.top_k).drop("rk"))
    bottom  = (weather_daily_enriched .withColumn("rk", F.row_number().over(wasc )).where(F.col("rk")<=args.top_k).drop("rk"))
    write_parquet(top, f"{cg.BASE_GOLD_BUCKET}/bqs/best_days_top",
                  parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)
    write_parquet(bottom, f"{cg.BASE_GOLD_BUCKET}/bqs/worst_days_top",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)

    whot  = W.partitionBy("city_name","year","month").orderBy(F.col("max_temp").desc())
    wcold = W.partitionBy("city_name","year","month").orderBy(F.col("min_temp").asc())
    hottest = (weather_daily_enriched .withColumn("rk", F.row_number().over(whot))
                     .where(F.col("rk")<=args.top_k).drop("rk"))
    coldest = (weather_daily_enriched .withColumn("rk", F.row_number().over(wcold))
                     .where(F.col("rk")<=args.top_k).drop("rk"))
    write_parquet(hottest, f"{cg.BASE_GOLD_BUCKET}/bqs/htemps_top",
                  parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)
    write_parquet(coldest, f"{cg.BASE_GOLD_BUCKET}/bqs/ltemps_top",
                  parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)

    ug.cleanup(dim_date, dim_city, dim_weather_condition)

    print("[OK] cg.BASE_GOLD_BUCKET construido en:", cg.BASE_GOLD_BUCKET)

# -------------------------------- CLI ---------------------------------------- #
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--compare-date", default=None, help="YYYY-MM-DD)")
    ap.add_argument("--years-keep",  default=None, help="2024,2025")
    ap.add_argument("--months-keep", default=None, help="8,9")
    ap.add_argument("--cities",      default=None, help='"Riohacha,Patagonia"')
    ap.add_argument("--only-answers", default=None)
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--top-include-low", action="store_true")
    ap.add_argument("--shuffle-partitions", type=int, default=16)
    ap.add_argument("--max-records-per-file", type=int, default=150000)
    args = ap.parse_args()
    main(args)