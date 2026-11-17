
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark
spark = (
    SparkSession.builder
    .appName("KafkaStreamingReader")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Read from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "order-events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# 3. Select Kafka metadata and message
result_df = kafka_df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value")
)

# 4. Write to console
query = (
    result_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/opt/spark/data/checkpoints/kafka_reader")
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
