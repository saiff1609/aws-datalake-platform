import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


# INITIALIZE GLUE CONTEXT


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=== Silver pipeline started ===")


# 1. LOAD DATA


df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(args['S3_INPUT_PATH'])

print("Data loaded.")


# 2. STANDARDIZE COLUMN NAMES


df = df.toDF(*[c.strip().lower() for c in df.columns])
print("Column standardization completed.")


# 3. TYPE CASTING


df = df.withColumn("fl_date", F.to_date(F.col("fl_date"), "yyyy-MM-dd"))

numeric_columns = [
    "dot_code", "fl_number", "crs_dep_time", "dep_time", "dep_delay",
    "taxi_out", "wheels_off", "wheels_on", "taxi_in",
    "crs_arr_time", "arr_time", "arr_delay",
    "cancelled", "diverted",
    "crs_elapsed_time", "elapsed_time", "air_time", "distance",
    "delay_due_carrier", "delay_due_weather",
    "delay_due_nas", "delay_due_security",
    "delay_due_late_aircraft"
]

for col_name in numeric_columns:
    if col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast(FloatType()))

print("Type casting completed.")


# 4. NULL HANDLING


important_columns = ["fl_date", "airline", "fl_number", "origin", "dest"]

df = df.dropna(subset=important_columns)
df = df.fillna({"cancelled": 0.0, "diverted": 0.0})

print("Null handling completed.")


# 5. REMOVE DUPLICATES


df = df.dropDuplicates(["fl_date", "airline", "fl_number", "origin", "dest"])
print("Duplicate removal completed.")


# 6. ADD INGESTION METADATA


df = df.withColumn("ingestion_timestamp", F.current_timestamp())
df = df.withColumn("data_source", F.lit("flights_2023_csv"))
df = df.withColumn("year", F.year(F.col("fl_date")))
df = df.withColumn("month", F.month(F.col("fl_date")))
df = df.withColumn("day", F.dayofmonth(F.col("fl_date")))

print("Metadata columns added.")


# 7. VALIDATION CHECKS


df.cache()

negative_distance = df.filter(F.col("distance") < 0).count()
if negative_distance > 0:
    raise ValueError(f"Validation failed: {negative_distance} rows with negative distance.")

invalid_cancelled = df.filter(
    (F.col("cancelled") == 1) & (F.col("air_time") > 0)
).count()
if invalid_cancelled > 0:
    print(f"Warning: {invalid_cancelled} cancelled flights with air_time detected.")

print("Data validation completed.")


# 8. SAVE SILVER LAYER


df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(args['S3_OUTPUT_PATH'])

print("Silver layer saved successfully.")


# COMMIT JOB


job.commit()
print("=== Silver pipeline completed ===")