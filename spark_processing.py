from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, month, to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Create Spark Session
spark = SparkSession.builder \
    .appName("Weather Data Processing") \
    .getOrCreate()

print("Spark Session Created Successfully")

# Read CSV using Spark
df = spark.read.csv("weather_data.csv", header=True, inferSchema=True)

# 🔹 Convert date column properly (IMPORTANT FIX)
df = df.withColumn("date", to_date("date", "dd-MM-yyyy"))

print("Spark Data Loaded:")
df.show()

print("Average Temperature Per City (Spark):")

avg_df = df.groupBy("city").agg(
    avg("temperature").alias("avg_temperature")
)

avg_df.show()

print("Monthly Average Temperature Per City (Spark):")

monthly_df = df.withColumn("month", month("date")) \
    .groupBy("city", "month") \
    .agg(avg("temperature").alias("avg_temperature"))

monthly_df.show()

print("Hottest Day Per City (Spark):")

window_spec = Window.partitionBy("city").orderBy(desc("temperature"))

ranked_df = df.withColumn("rank", row_number().over(window_spec))

hottest_df = ranked_df.filter(ranked_df.rank == 1)

hottest_df.select("city", "date", "temperature").show()