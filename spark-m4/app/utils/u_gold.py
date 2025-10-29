from pyspark.sql import functions as F

def filter_and_cache(df, years, months, cities):
    cond = F.lit(True)
    
    if years:
        year_list = [int(x.strip()) for x in years.split(",") if x.strip()]
        cond = cond & F.col("event_year").isin(year_list)
    
    if months:
        month_list = [int(x.strip()) for x in months.split(",") if x.strip()]
        cond = cond & F.col("event_month").isin(month_list)
    
    if cities:
        city_list = [x.strip() for x in cities.split(",") if x.strip()]
        cond = cond & F.col("city").isin(city_list)
    
    filtered_df = df.where(cond).cache()
    return filtered_df

def prepare_dim(df, select_cols):
    df_cached = cache_and_count(df)
    return F.broadcast(df_cached.select(*select_cols))

def cache_and_count(df):
    df.cache()
    df.count()  
    return df

def cleanup(*dfs):
    for df in dfs:
        df.unpersist(False)


def hash_key(col, max_value=1000000):
    col_expr = col if isinstance(col, F.Column) else F.col(col)
    return (F.abs(F.hash(col_expr)) % F.lit(max_value)).cast("int")

def compute_percentiles(df, col_name, group_cols=["city","event_month"], probs=[0.05,0.95], accuracy=250):
    probs_array = f"array({','.join(map(str, probs))})"
    return (
        df.groupBy(*group_cols)
        .agg(F.expr(f"percentile_approx({col_name}, {probs_array}, 250) as pct"))
          .withColumn("p5",  F.col("pct").getItem(0))
          .withColumn("p95", F.col("pct").getItem(1))
          .drop("pct")
    )

def normalize_with_guard(num, lo, hi):
    denom = (hi - lo)
    return F.when(denom.isNull() | (F.abs(denom) < 1e-9), F.lit(0.0)) \
            .otherwise((num - lo)/denom)

def bound_to_unit_interval(col):
    return F.when(col < 0, 0.0).when(col > 1, 1.0).otherwise(col)
