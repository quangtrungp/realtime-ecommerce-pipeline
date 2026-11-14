
import random
from uuid import uuid4

from model_generator.payment import create_payment_event


class PaymentGenerator:

    def __init__(
        self,
        order_registry,
        max_attempts=3,
        success_rate=0.75
    ):
        self.orders = order_registry
        self.max_attempts = max_attempts
        self.success_rate = success_rate

    def generate_payment_event(
        self,
        event_type,
        payment_id,
        order,
        status,
        failure_reason=None
    ):

        return create_payment_event(
            event_type=event_type,
            payment_id=payment_id,
            order_id=order["order_id"],
            payment_method="credit_card",
            amount=order["total_amount"],
            status=status,
            failure_reason=failure_reason
        )

    def process_payment(self, order_id):

        if order_id not in self.orders:
            raise ValueError(
                f"Order {order_id} does not exist"
            )

        order = self.orders[order_id]

        if order["status"] != "created":
            raise ValueError(
                "Cannot pay an order that is not created"
            )

        events = []

        for attempt in range(1, self.max_attempts + 1):

            payment_id = f"pay_{uuid4().hex[:12]}"

            order["payment_attempts"].append(payment_id)

            # 1. Payment initiated
            initiated_event = self.generate_payment_event(
                event_type="payment_initiated",
                payment_id=payment_id,
                order=order,
                status="initiated"
            )

            events.append(initiated_event)

            # 2. Simulate payment result
            if random.random() < self.success_rate:

                completed_event = self.generate_payment_event(
                    event_type="payment_completed",
                    payment_id=payment_id,
                    order=order,
                    status="completed"
                )

                events.append(completed_event)

                order["payment_status"] = "completed"
                order["successful_payment_id"] = payment_id

                return events

            # 3. Payment failed
            failed_event = self.generate_payment_event(
                event_type="payment_failed",
                payment_id=payment_id,
                order=order,
                status="failed",
                failure_reason="payment_declined"
            )

            events.append(failed_event)

        # All attempts failed
        order["payment_status"] = "failed"

        return events
