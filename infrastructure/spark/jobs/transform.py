from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import ORDER_EVENT_SCHEMA, PAYMENT_EVENT_SCHEMA
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import functions as F
import os
from sqlalchemy import create_engine, text, URL
import psycopg2
# 1. Initialize Spark
spark = (
    SparkSession.builder
    .appName("Transform_Data")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Set up Connections
## MinIO

minio_endpoint = os.environ["MINIO_ENDPOINT"]
minio_access_key = os.environ["MINIO_ACCESS_KEY"]
minio_secret_key = os.environ["MINIO_SECRET_KEY"]
minio_bucket = os.environ["MINIO_BUCKET"]

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration() #type: ignore

hadoop_conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem",
)

hadoop_conf.set(
    "fs.s3a.endpoint",
    minio_endpoint,
)

hadoop_conf.set(
    "fs.s3a.access.key",
    minio_access_key,
)

hadoop_conf.set(
    "fs.s3a.secret.key",
    minio_secret_key,
)

hadoop_conf.set(
    "fs.s3a.path.style.access",
    "true",
)

hadoop_conf.set(
    "fs.s3a.connection.ssl.enabled",
    "false",
)

hadoop_conf.set(
    "fs.s3a.endpoint.region",
    "us-east-1",
)

hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
)

def load_raw_order_minio(batch_df, batch_id):
    output_path = f"s3a://{minio_bucket}/order/raw/batch_id={batch_id}"
    batch_df.write.mode("overwrite").parquet(output_path)


def load_raw_payment_minio(batch_df, batch_id):
    output_path = f"s3a://{minio_bucket}/payment/raw/batch_id={batch_id}"
    batch_df.write.mode("overwrite").parquet(output_path)


def load_cleaned_order_minio(batch_df, batch_id):
    output_path = f"s3a://{minio_bucket}/order/cleaned/batch_id={batch_id}"
    batch_df.write.mode("overwrite").parquet(output_path)


def load_cleaned_payment_minio(batch_df, batch_id):
    output_path = f"s3a://{minio_bucket}/payment/cleaned/batch_id={batch_id}"
    batch_df.write.mode("overwrite").parquet(output_path)

## PostgreSQL

jdbc_url = os.environ["POSTGRES_JDBC_URL"]
postgres_user = os.environ["POSTGRES_USER"]
postgres_password = os.environ["POSTGRES_PASSWORD"]

jdbc_options = {
    "url": jdbc_url,
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
}

postgres_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host="host.docker.internal",
    port=5432,
    database=os.getenv("POSTGRES_DATABASE")
)

postgres_engine = create_engine(postgres_url, pool_pre_ping=True)

def load_order_postgres(batch_df, batch_id):
    (
    batch_df
    .write
    .format("jdbc")
    .options(**jdbc_options)
    .option("dbtable", "staging.orders")
    .option("truncate", "true")
    .mode("overwrite")
    .save()
    )
    call_order_procedure()

def load_payment_postgres(batch_df, batch_id):
    (
    batch_df
    .write
    .format("jdbc")
    .options(**jdbc_options)
    .option("dbtable", "staging.payments")
    .option("truncate", "true")
    .mode("overwrite")
    .save()
    )
    call_payment_procedure()

def call_order_procedure():

    with postgres_engine.begin() as conn:
        conn.execute(
            text("CALL analytics.load_orders()")
        )

def call_payment_procedure():

    with postgres_engine.begin() as conn:
        conn.execute(
            text("CALL analytics.load_payments()")
        )

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
dedup_order=parsed_order.dropDuplicates(["event_id"])

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
dedup_payment=parsed_payment.dropDuplicates(["event_id"])

# 5. Select columns
raw_order_df = dedup_order.select(
    col("payload.order_id").alias("order_id"),
    col("payload.user_id").alias("user_id"),
    col("event_id"),
    F.explode_outer("payload.items").alias("item"),
    col("payload.total_amount").alias("total_amount"),
    col("payload.currency").alias("currency"),
    col("event_type"),
    col("event_time")
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

raw_payment_df = dedup_payment.select(
    col("payload.payment_id").alias("payment_id"),
    col("payload.order_id").alias("order_id"),
    col("event_id"),
    col("event_time"),
    col("event_type"),
    col("payload.payment_method").alias("payment_method"),
    col("payload.amount").alias("amount"),
    col("payload.currency").alias("currency"),
    col("payload.status").alias("status"),
    col("payload.failure_reason").alias("failure_reason"),
    col("payload.refund_reason").alias("refund_reason")
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

# 7. Streamming

## orders
raw_order_query=(
    explode_item.writeStream
    .outputMode("append")
    .foreachBatch(load_raw_order_minio)
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/raw_order_parsed"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

cleaned_order_query=(
    order_df.writeStream
    .outputMode("append")
    .foreachBatch(load_cleaned_order_minio)
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/cleaned_order_parsed"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

order_query=(
    order_df.writeStream
    .outputMode("append")
    .foreachBatch(load_order_postgres)
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/order_parsed"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

## payments

raw_payment_query=(
    raw_payment_df.writeStream
    .outputMode("append")
    .foreachBatch(load_raw_payment_minio)
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/raw_payment_parsed"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

cleaned_payment_query=(
    payment_df.writeStream
    .outputMode("append")
    .foreachBatch(load_cleaned_payment_minio)
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/cleaned_payment_parsed"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

payment_query=(
    payment_df.writeStream
    .outputMode("append")
    .foreachBatch(load_payment_postgres)
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/payment_parsed"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

## Await

spark.streams.awaitAnyTermination()