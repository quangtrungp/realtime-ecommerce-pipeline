
from decimal import Decimal
from .common import create_event, decimal_to_string


def create_order_payload(
    order_id: str,
    user_id: int,
    items: list[dict],
    currency: str = "VND"
) -> dict:

    total_amount = Decimal(
        sum(
            Decimal(str(item["unit_price"])) * item["quantity"] for item in items
        )
    )

    return {
        "order_id": order_id,
        "user_id": user_id,
        "items": [
            {
                "product_id": item["product_id"],
                "quantity": item["quantity"],
                "unit_price": decimal_to_string(
                    Decimal(str(item["unit_price"]))
                )
            }
            for item in items
        ],
        "total_amount": decimal_to_string(total_amount),
        "currency": currency
    }


def create_order_created_event(
    order_id: str,
    user_id: int,
    items: list[dict]
) -> dict:

    payload = create_order_payload(
        order_id=order_id,
        user_id=user_id,
        items=items
    )

    return create_event(
        event_type="order_created",
        source="order_generator",
        payload=payload
    )


def create_order_cancelled_event(
    order_id: str,
    reason: str,
    cancelled_by: str
) -> dict:

    payload = {
        "order_id": order_id,
        "reason": reason,
        "cancelled_by": cancelled_by
    }

    return create_event(
        event_type="order_cancelled",
        source="order_generator",
        payload=payload
    )


def create_order_completed_event(
    order_id: str,
    completed_at: str
) -> dict:

    payload = {
        "order_id": order_id,
        "completed_at": completed_at
    }

    return create_event(
        event_type="order_completed",
        source="order_generator",
        payload=payload,
        event_time=completed_at
    )
