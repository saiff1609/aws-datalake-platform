CREATE DATABASE datalake_flights;


CREATE EXTERNAL TABLE datalake_flights.avg_delay_by_airline (
  airline STRING, avg_arr_delay DOUBLE, avg_dep_delay DOUBLE,
  total_flights BIGINT, ingestion_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://datalake-pipeline-project/gold/flights/avg_delay_by_airline/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


CREATE EXTERNAL TABLE datalake_flights.cancellation_by_airline (
  airline STRING, total_flights BIGINT, total_cancelled DOUBLE,
  cancellation_rate_pct DOUBLE, ingestion_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://datalake-pipeline-project/gold/flights/cancellation_by_airline/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


CREATE EXTERNAL TABLE datalake_flights.delay_cause_breakdown (
  delay_cause STRING, total_delay_minutes DOUBLE, ingestion_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://datalake-pipeline-project/gold/flights/delay_cause_breakdown/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


CREATE EXTERNAL TABLE datalake_flights.most_delayed_routes (
  origin STRING, dest STRING, avg_arr_delay DOUBLE,
  total_flights BIGINT, ingestion_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://datalake-pipeline-project/gold/flights/most_delayed_routes/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


CREATE EXTERNAL TABLE datalake_flights.monthly_delay_trend (
  month INT, avg_arr_delay DOUBLE, avg_dep_delay DOUBLE,
  total_flights BIGINT, ingestion_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://datalake-pipeline-project/gold/flights/monthly_delay_trend/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- Silver layer — partitioned by year/month/day matching glue output structure for easier querying and partition pruning.
CREATE EXTERNAL TABLE datalake_flights.silver_flights (
  fl_date DATE, airline STRING, airline_code STRING, origin STRING,
  dest STRING, dep_delay FLOAT, arr_delay FLOAT, cancelled FLOAT,
  distance FLOAT, delay_due_carrier FLOAT, delay_due_weather FLOAT,
  delay_due_nas FLOAT, delay_due_security FLOAT, delay_due_late_aircraft FLOAT,
  ingestion_timestamp TIMESTAMP, data_source STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://datalake-pipeline-project/silver/flights/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- Registers all existing partitions in glue data catalog
MSCK REPAIR TABLE datalake_flights.silver_flights;


SELECT * FROM datalake_flights.avg_delay_by_airline;

SELECT * FROM datalake_flights.monthly_delay_trend;

-- Partition pruning — compare data scanned between these two queries.

SELECT * FROM datalake_flights.silver_flights LIMIT 10;

SELECT * FROM datalake_flights.silver_flights
WHERE year = 2023 AND month = 1 AND day = 1
LIMIT 10;