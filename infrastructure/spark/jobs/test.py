from pyspark.sql.functions import from_json, col
from schema import ORDER_EVENT_SCHEMA
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

parsed_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        ORDER_EVENT_SCHEMA
    ).alias("event")
)

result_df = parsed_df.select(
    "event.event_id",
    "event.event_type",
    "event.event_time",
    "event.payload.order_id",
    "event.payload.user_id",
    "event.payload.total_amount"
)

query = (
    result_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/order_parsed"
    )
    .start()
)

query.awaitTermination()