from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import ORDER_EVENT_SCHEMA, PAYMENT_EVENT_SCHEMA
from pyspark.sql.functions import col
from pyspark.sql import functions as F
# 1. Initialize Spark
spark = (
    SparkSession.builder
    .appName("Transform_Data")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2.

# 3. Stream
order_events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "order-events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

payment_events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "payment-events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4. Parse df
parsed_order = order_events.select(
    from_json(
        col("value").cast("string"),
        ORDER_EVENT_SCHEMA
    ).alias("order_events")
)

parsed_payment = payment_events.select(
    from_json(
        col("value").cast("string"),
        PAYMENT_EVENT_SCHEMA
    ).alias("payment_events")
)

# 5. Select columns
raw_order_df=parsed_order.select(
    col("order_events.order_id").alias("order_id"),
    col("order_events.user_id").alias("user_id"),
    col("order_events.event_id").alias("event_id"),
    F.explode("order_events.items").alias("item"),
    col("order_events.total_amount").alias("total_amount"),
    col("order_events.currency").alias("currency"),
    col("order_events.event_type").alias("event_type"),
    col("order_events.event_time").alias("event_time"),

).dropDuplicates(["order_events.event_id"])
explode_item=raw_order_df.select(
    col("order_id"),
    col("user_id"),
    col("event_id"),
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity"),
    col("item.unit_price").alias("unit_price"),
    col("total_amount"),
    col("currency"),
    col("event_time"),
    col("event_type"),

)

raw_payment_df=parsed_payment.select(
    col("payment_events.payment_id").alias("payment_id"),
    col("payment_events.order_id").alias("order_id"),
    col("payment_events.event_id").alias("event_id"),
    col("payment_events.payment_method").alias("payment_method"),
    col("payment_events.amount").alias("amount"),
    col("payment_events.currency").alias("currency"),
    col("payment_events.status").alias("status"),
    col("payment_events.failure_reason").alias("failure_reason"),
    col("payment_events.refund_reason").alias("refund_reason"),
    col("payment_events.event_time").alias("event_time"),
    col("payment_events.event_type").alias("event_type"),

).dropDuplicates(["payment_events.event_id"])


