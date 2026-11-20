from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import ORDER_EVENT_SCHEMA, PAYMENT_EVENT_SCHEMA
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import functions as F
# 1. Initialize Spark
spark = (
    SparkSession.builder
    .appName("Transform_Data")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Set up Connections

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
parsed_order = (
    order_events
    .select(
        from_json(
            col("value").cast("string"),
            ORDER_EVENT_SCHEMA
        ).alias("order_events")
    )
    .select("order_events.*")
    .withColumn("event_time", to_timestamp("event_time")) # phai de vi can chuyen sang kdl timestamp
    .withWatermark("event_time", "10 minutes")
)
deup_order=parsed_order.dropDuplicates(["event_id"])

parsed_payment = (
    payment_events
    .select(
        from_json(
            col("value").cast("string"),
            PAYMENT_EVENT_SCHEMA
        ).alias("payment_events")
    )
    .select("payment_events.*")
    .withColumn("event_time", to_timestamp("event_time"))
    .withWatermark("event_time", "10 minutes")
)
deup_payment=parsed_payment.dropDuplicates(["event_id"])

# 5. Select columns
raw_order_df=deup_order.select(
    col("order_id"),
    col("user_id"),
    col("event_id"),
    F.explode_outer("items").alias("item"),
    col("total_amount"),
    col("currency"),
    col("event_type"),
    col("event_time"),

)

explode_item=raw_order_df.select(
    col("order_id"),
    col("user_id"),
    col("event_id"),
    col("event_time"),
    col("event_type"),
    col("total_amount"),
    col("currency"),
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity"),
    col("item.unit_price").alias("unit_price"),
    
)

raw_payment_df=deup_payment.select(
    col("payment_id"),
    col("order_id"),
    col("event_id"),
    col("event_time"),
    col("event_type"),
    col("payment_method"),
    col("amount"),
    col("currency"),
    col("status"),
    col("failure_reason"),
    col("refund_reason"),

)

# 6. Filter

order_df=explode_item.filter(
    col("order_id").isNotNull() &
    ( (col("quantity") > 0) | col("quantity").isNull() ) &
    ( (col("unit_price") > 0) | col("unit_price").isNull() ) &
    ( (col("total_amount") > 0) | col("total_amount").isNull() )
)

payment_df=raw_payment_df.filter(
    col("payment_id").isNotNull() &
    col("order_id").isNotNull() &
    col("amount").isNotNull() &
    (col("amount") > 0)
)