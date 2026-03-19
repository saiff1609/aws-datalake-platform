import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


# INITIALIZE GLUE CONTEXT


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=== Gold pipeline started ===")


# LOAD SILVER DATA


df = spark.read.parquet(args['S3_INPUT_PATH'])
row_count = df.count()
df.cache()

print(f"Silver data loaded | Rows: {row_count}")


# AGG 1 — AVERAGE DELAY BY AIRLINE


agg1 = df.groupBy("airline").agg(
    F.round(F.mean("arr_delay"), 2).alias("avg_arr_delay"),
    F.round(F.mean("dep_delay"), 2).alias("avg_dep_delay"),
    F.count("fl_number").alias("total_flights")
).orderBy(F.col("avg_arr_delay").desc())


# AGG 2 — CANCELLATION RATE BY AIRLINE


agg2 = df.groupBy("airline").agg(
    F.count("fl_number").alias("total_flights"),
    F.sum("cancelled").alias("total_cancelled")
).withColumn(
    "cancellation_rate_pct",
    F.round((F.col("total_cancelled") / F.col("total_flights")) * 100, 2)
).orderBy(F.col("cancellation_rate_pct").desc())


# AGG 3 — DELAY CAUSE BREAKDOWN


delay_cols = [
    "delay_due_carrier",
    "delay_due_weather",
    "delay_due_nas",
    "delay_due_security",
    "delay_due_late_aircraft"
]



agg3 = df.agg(*[F.round(F.sum(c), 0).alias(c) for c in delay_cols])
agg3 = agg3.unpivot([], delay_cols, "delay_cause", "total_delay_minutes")

agg3 = agg3.replace({
    'delay_due_late_aircraft': 'Late Aircraft',
    'delay_due_carrier': 'Carrier',
    'delay_due_nas': 'NAS',
    'delay_due_weather': 'Weather',
    'delay_due_security': 'Security'
})

agg3 = agg3.orderBy(F.col("total_delay_minutes").desc())


# AGG 4 — MOST DELAYED ROUTES


agg4 = df.groupBy("origin", "dest").agg(
    F.round(F.mean("arr_delay"), 2).alias("avg_arr_delay"),
    F.count("fl_number").alias("total_flights")
).filter(F.col("total_flights") >= 10) \
 .orderBy(F.col("avg_arr_delay").desc()) \
 .limit(20)


# AGG 5 — MONTHLY DELAY TREND


agg5 = df.groupBy("month").agg(
    F.round(F.mean("arr_delay"), 2).alias("avg_arr_delay"),
    F.round(F.mean("dep_delay"), 2).alias("avg_dep_delay"),
    F.count("fl_number").alias("total_flights")
).orderBy("month")

print("All aggregations computed.")


# ADD PIPELINE TIMESTAMP


run_timestamp = F.current_timestamp()

agg1 = agg1.withColumn("ingestion_timestamp", run_timestamp)
agg2 = agg2.withColumn("ingestion_timestamp", run_timestamp)
agg3 = agg3.withColumn("ingestion_timestamp", run_timestamp)
agg4 = agg4.withColumn("ingestion_timestamp", run_timestamp)
agg5 = agg5.withColumn("ingestion_timestamp", run_timestamp)


# SAVE GOLD DATASETS


output_path = args['S3_OUTPUT_PATH'].rstrip('/')

datasets = {
    "avg_delay_by_airline": agg1,
    "cancellation_by_airline": agg2,
    "delay_cause_breakdown": agg3,
    "most_delayed_routes": agg4,
    "monthly_delay_trend": agg5
}

for name, data in datasets.items():
    path = f"{output_path}/{name}/"
    data.coalesce(1).write.mode("overwrite").parquet(path)
    print(f"Saved: {path}")


# COMMIT JOB


job.commit()
print("=== Gold pipeline completed ===")