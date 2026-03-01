from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .appName("WeatherStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "C:/temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Streaming Spark Session Created")

# Built-in rate streaming source
stream_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 2) \
    .load()

# Simple transformation
result = stream_df.selectExpr(
    "value",
    "timestamp",
    "value % 5 as temperature_simulated"
)

query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()