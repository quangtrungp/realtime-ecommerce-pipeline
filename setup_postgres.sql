# Run commands one by one
create database ecommerce;

create schema ecommerce.staging;

create schema ecommerce.analytics;

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id        VARCHAR(100),
    user_id         VARCHAR(100),
    event_id        VARCHAR(100),
    event_time      TIMESTAMP,
    event_type      VARCHAR(50),
    total_amount    NUMERIC(18, 2),
    currency        VARCHAR(10),
    product_id      VARCHAR(100),
    quantity        INTEGER,
    unit_price      NUMERIC(18, 2)
);

CREATE TABLE IF NOT EXISTS staging.payments (
    payment_id      VARCHAR(100),
    order_id        VARCHAR(100),
    event_id        VARCHAR(100),
    event_time      TIMESTAMP,
    event_type      VARCHAR(50),
    payment_method  VARCHAR(50),
    amount          NUMERIC(18, 2),
    currency        VARCHAR(10),
    status          VARCHAR(30),
    failure_reason  TEXT,
    refund_reason   TEXT
);

CREATE TABLE IF NOT EXISTS analytics.dim_orders (
    order_id       VARCHAR(100) PRIMARY KEY,
    user_id        VARCHAR(100),
    currency       VARCHAR(10),
    total_amount   NUMERIC(18, 2),
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.dim_payments (
    payment_id          VARCHAR(100) PRIMARY KEY,
    order_id            VARCHAR(100) NOT NULL,
    payment_method      VARCHAR(50),
    currency            VARCHAR(10),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.fact_order_items (
    order_item_key  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id        VARCHAR(100) NOT NULL,
    product_id      VARCHAR(100) NOT NULL,
    event_id        VARCHAR(100) NOT NULL,
    event_time      TIMESTAMP,
    event_type      VARCHAR(50),
    quantity        INTEGER,
    unit_price      NUMERIC(18, 2),
    total_amount    NUMERIC(18, 2),
    currency        VARCHAR(10),

    FOREIGN KEY (order_id)
        REFERENCES analytics.dim_orders(order_id)
);

REATE TABLE IF NOT EXISTS analytics.fact_payments (
    payment_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id       VARCHAR(100) NOT NULL,
    order_id         VARCHAR(100) NOT NULL,
    event_id         VARCHAR(100) NOT NULL,
    event_time       TIMESTAMP,
    event_type       VARCHAR(50),
    amount           NUMERIC(18, 2),
    currency         VARCHAR(10),
    status           VARCHAR(50),
    failure_reason   TEXT,
    refund_reason    TEXT,

    FOREIGN KEY (payment_id)
        REFERENCES analytics.dim_payments(payment_id),

    FOREIGN KEY (order_id)
        REFERENCES analytics.dim_orders(order_id)
);

ALTER TABLE analytics.fact_order_items
ADD CONSTRAINT uq_fact_order_items
UNIQUE (order_id, product_id, event_id);

ALTER TABLE analytics.fact_payments
ADD CONSTRAINT uq_fact_payments
UNIQUE (payment_id, event_id);