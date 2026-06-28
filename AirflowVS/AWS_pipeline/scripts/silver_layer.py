import sys
from pyspark import SparkConf
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'load_date'])
load_date = args['load_date']
print(f"load date: {load_date}")

conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


df_bookings = spark.read.format("csv") \
              .option("inferSchema", True) \
              .option("header", True) \
              .load(f"s3://airflow-aws-ananda/bronze/{load_date}/bookings.csv")

df_airports = spark.read.format("csv") \
              .option("inferSchema", True) \
              .option("header", True) \
              .load(f"s3://airflow-aws-ananda/bronze/{load_date}/airports.csv")

df_passengers = spark.read.format("csv") \
              .option("inferSchema", True) \
              .option("header", True) \
              .load(f"s3://airflow-aws-ananda/bronze/{load_date}/passengers.csv")

df_full = (
    df_bookings
    .join(df_airports, df_bookings["airport_id"] == df_airports["airport_id"], "left")
    .drop(df_airports["airport_id"])
    .join(df_passengers, df_bookings["passenger_id"] == df_passengers["passenger_id"], "left")
    .drop(df_passengers["passenger_id"])
)

df_full = df_full.withColumn("processed_at", current_timestamp())

# write data through silver layer in DELTA format
df_full.write.format("delta").mode("append").option("path", "s3://airflow-aws-ananda/silver/obt").save()

job.commit()
