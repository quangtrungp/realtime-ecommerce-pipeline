# Run commands one by one

## create database
create database ecommerce;

## create schema
create schema staging;

create schema analytics;

## create table for staging
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

## create table for analytics
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

CREATE TABLE IF NOT EXISTS analytics.fact_payments (
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

## create store procedure
CREATE OR REPLACE PROCEDURE analytics.load_orders()
LANGUAGE plpgsql
AS $$
BEGIN

    -- STEP 1: UPDATE existing orders
    WITH latest_orders AS (
        SELECT DISTINCT ON (order_id)
            order_id,
            user_id,
            currency,
            total_amount,
            event_time
        FROM staging.orders
        WHERE event_time IS NOT NULL
        ORDER BY
            order_id,
            event_time DESC,
            event_id DESC
    )

    UPDATE analytics.dim_orders AS d
    SET
        user_id      = s.user_id,
        currency     = s.currency,
        total_amount = s.total_amount,
        updated_at   = s.event_time
    FROM latest_orders AS s
    WHERE
        d.order_id = s.order_id

        -- Event must be newer than the last recorded change
        AND s.event_time > COALESCE(d.updated_at, d.created_at)

        -- Only update when attributes actually change
        AND (
            d.user_id      IS DISTINCT FROM s.user_id
            OR d.currency     IS DISTINCT FROM s.currency
            OR d.total_amount IS DISTINCT FROM s.total_amount
        );


    -- STEP 2: INSERT new orders
    WITH latest_orders AS (
        SELECT DISTINCT ON (order_id)
            order_id,
            user_id,
            currency,
            total_amount,
            event_time
        FROM staging.orders
        WHERE event_time IS NOT NULL
        ORDER BY
            order_id,
            event_time DESC,
            event_id DESC
    )

    INSERT INTO analytics.dim_orders (
        order_id,
        user_id,
        currency,
        total_amount,
        created_at,
        updated_at
    )
    SELECT
        s.order_id,
        s.user_id,
        s.currency,
        s.total_amount,
        s.event_time,
        NULL
    FROM latest_orders AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM analytics.dim_orders AS d
        WHERE d.order_id = s.order_id
    );

END;
$$;

CREATE OR REPLACE PROCEDURE analytics.load_payments()
LANGUAGE plpgsql
AS $$
BEGIN

    -- STEP 1: UPDATE existing payments
    WITH latest_payments AS (
        SELECT DISTINCT ON (payment_id)
            payment_id,
            order_id,
            payment_method,
            currency,
            event_time
        FROM staging.payments
        WHERE event_time IS NOT NULL
        ORDER BY
            payment_id,
            event_time DESC,
            event_id DESC
    )

    UPDATE analytics.dim_payments AS d
    SET
        order_id       = s.order_id,
        payment_method = s.payment_method,
        currency       = s.currency,
        updated_at     = s.event_time
    FROM latest_payments AS s
    WHERE
        d.payment_id = s.payment_id

        AND s.event_time > COALESCE(d.updated_at, d.created_at)

        AND (
            d.order_id       IS DISTINCT FROM s.order_id
            OR d.payment_method IS DISTINCT FROM s.payment_method
            OR d.currency       IS DISTINCT FROM s.currency
        );


    -- STEP 2: INSERT new payments
    WITH latest_payments AS (
        SELECT DISTINCT ON (payment_id)
            payment_id,
            order_id,
            payment_method,
            currency,
            event_time
        FROM staging.payments
        WHERE event_time IS NOT NULL
        ORDER BY
            payment_id,
            event_time DESC,
            event_id DESC
    )

    INSERT INTO analytics.dim_payments (
        payment_id,
        order_id,
        payment_method,
        currency,
        created_at,
        updated_at
    )
    SELECT
        s.payment_id,
        s.order_id,
        s.payment_method,
        s.currency,
        s.event_time,
        NULL
    FROM latest_payments AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM analytics.dim_payments AS d
        WHERE d.payment_id = s.payment_id
    );

END;
$$;

CREATE OR REPLACE PROCEDURE analytics.load_fact_order_items()
LANGUAGE plpgsql
AS $$
BEGIN

    INSERT INTO analytics.fact_order_items (
        order_id,
        product_id,
        event_id,
        event_time,
        event_type,
        quantity,
        unit_price,
        total_amount,
        currency
    )

    SELECT DISTINCT ON (
        s.order_id,
        s.product_id,
        s.event_id
    )
        s.order_id,
        s.product_id,
        s.event_id,
        s.event_time,
        s.event_type,
        s.quantity,
        s.unit_price,
        s.quantity * s.unit_price AS total_amount,
        s.currency

    FROM staging.orders AS s

    INNER JOIN analytics.dim_orders AS d
        ON s.order_id = d.order_id

    WHERE
        s.order_id IS NOT NULL
        AND s.product_id IS NOT NULL
        AND s.event_id IS NOT NULL
        AND s.event_time IS NOT NULL

    ORDER BY
        s.order_id,
        s.product_id,
        s.event_id,
        s.event_time DESC

    ON CONFLICT (order_id, product_id, event_id)

    DO UPDATE SET
        event_time   = EXCLUDED.event_time,
        event_type   = EXCLUDED.event_type,
        quantity     = EXCLUDED.quantity,
        unit_price   = EXCLUDED.unit_price,
        total_amount = EXCLUDED.total_amount,
        currency     = EXCLUDED.currency

    WHERE
        analytics.fact_order_items.event_time IS NULL
        OR EXCLUDED.event_time >= analytics.fact_order_items.event_time;

END;
$$;

CREATE OR REPLACE PROCEDURE analytics.load_fact_payments()
LANGUAGE plpgsql
AS $$
BEGIN

    INSERT INTO analytics.fact_payments (
        payment_id,
        order_id,
        event_id,
        event_time,
        event_type,
        amount,
        currency,
        status,
        failure_reason,
        refund_reason
    )

    SELECT DISTINCT ON (
        s.payment_id,
        s.event_id
    )
        s.payment_id,
        s.order_id,
        s.event_id,
        s.event_time,
        s.event_type,
        s.amount,
        s.currency,
        s.status,
        s.failure_reason,
        s.refund_reason

    FROM staging.payments AS s

    INNER JOIN analytics.dim_payments AS dp
        ON s.payment_id = dp.payment_id
        AND s.order_id = dp.order_id

    INNER JOIN analytics.dim_orders AS dor
        ON dp.order_id = dor.order_id

    WHERE
        s.payment_id IS NOT NULL
        AND s.order_id IS NOT NULL
        AND s.event_id IS NOT NULL
        AND s.event_time IS NOT NULL

    ORDER BY
        s.payment_id,
        s.event_id,
        s.event_time DESC

    ON CONFLICT (payment_id, event_id)

    DO UPDATE SET
        order_id       = EXCLUDED.order_id,
        event_time     = EXCLUDED.event_time,
        event_type     = EXCLUDED.event_type,
        amount         = EXCLUDED.amount,
        currency       = EXCLUDED.currency,
        status         = EXCLUDED.status,
        failure_reason = EXCLUDED.failure_reason,
        refund_reason  = EXCLUDED.refund_reason

    WHERE
        analytics.fact_payments.event_time IS NULL
        OR EXCLUDED.event_time >= analytics.fact_payments.event_time;

END;
$$;