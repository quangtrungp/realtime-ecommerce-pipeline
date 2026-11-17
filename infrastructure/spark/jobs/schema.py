from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    ArrayType
)

# Common event envelope
EVENT_ENVELOPE = [
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_version", IntegerType(), False),
    StructField("event_time", StringType(), False),
    StructField("produced_at", StringType(), False),
    StructField("source", StringType(), False)
]

# Order item
ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(18, 2), True)
])

# Order payload
ORDER_PAYLOAD_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField(
        "items",
        ArrayType(ORDER_ITEM_SCHEMA),
        True
    ),
    StructField("total_amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True)
])

# Payment payload
PAYMENT_PAYLOAD_SCHEMA = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("refund_reason", StringType(), True)
])

# Complete schemas
ORDER_EVENT_SCHEMA = StructType(
    EVENT_ENVELOPE + [
        StructField("payload", ORDER_PAYLOAD_SCHEMA, True)
    ]
)

PAYMENT_EVENT_SCHEMA = StructType(
    EVENT_ENVELOPE + [
        StructField("payload", PAYMENT_PAYLOAD_SCHEMA, True)
    ]
)