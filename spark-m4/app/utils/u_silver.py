from pyspark.sql import functions as F, types as T

def extract_hourly_precipitation  (df, root_col):
    field = next((f for f in df.schema.fields if f.name == root_col), None)
    if field and isinstance(field.dataType, T.StructType):
        names = {f.name for f in field.dataType.fields}
        if "1h" in names:
            return F.col(root_col).getField("1h").cast("double")
        if "_1h" in names:
            return F.col(root_col).getField("_1h").cast("double")
    return F.lit(0.0)

def categorize_numeric(col, limits):
    return (
        F.when(col.isNull(), F.lit(None))
         .when(col < limits[0], F.lit(f"<{limits[0]}"))
         .when(col < limits[1], F.lit(f"{limits[0]}-{limits[1]}"))
         .when(col < limits[2], F.lit(f"{limits[1]}-{limits[2]}"))
         .when(col < limits[3], F.lit(f"{limits[2]}-{limits[3]}"))
         .otherwise(F.lit(f"{limits[3]}+"))
    )