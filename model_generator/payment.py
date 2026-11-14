
from decimal import Decimal
from .common import create_event, decimal_to_string


def create_payment_payload(
    payment_id: str,
    order_id: str,
    payment_method: str,
    amount: Decimal,
    status: str,
    currency: str = "VND",
    failure_reason: str | None = None,
    refund_reason: str | None = None
) -> dict:

    payload = {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_method": payment_method,
        "amount": decimal_to_string(amount),
        "currency": currency,
        "status": status
    }

    if failure_reason:
        payload["failure_reason"] = failure_reason

    if refund_reason:
        payload["refund_reason"] = refund_reason

    return payload


def create_payment_event(
    event_type: str,
    payment_id: str,
    order_id: str,
    payment_method: str,
    amount: Decimal,
    status: str,
    failure_reason: str | None = None,
    refund_reason: str | None = None
) -> dict:

    payload = create_payment_payload(
        payment_id=payment_id,
        order_id=order_id,
        payment_method=payment_method,
        amount=amount,
        status=status,
        failure_reason=failure_reason,
        refund_reason=refund_reason
    )

    return create_event(
        event_type=event_type,
        source="payment_generator",
        payload=payload
    )
