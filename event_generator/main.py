
from .order_generator import OrderGenerator
from .payment_generator import PaymentGenerator


def main():

    order_generator = OrderGenerator()

    payment_generator = PaymentGenerator(
        order_registry=order_generator.orders,
        max_attempts=3,
        success_rate=0.75
    )

    all_events = []

    # Generate 10 orders
    for _ in range(1):

        order_event = order_generator.generate_order()

        order_id = order_event["payload"]["order_id"]

        all_events.append(order_event)

        payment_events = payment_generator.process_payment(order_id)
        all_events.extend(payment_events)

        payment_succeeded = any(
            event["event_type"] == "payment_completed"
            for event in payment_events
        )

        final_status = (
            "completed" if payment_succeeded else "cancelled"
        )

        final_event = order_generator.generate_order_final_event(
            order_id,
            final_status=final_status
        )

        all_events.append(final_event)

    # Print events
    for event in all_events:
        print(
            event["event_type"],
            event["payload"]
        )


if __name__ == "__main__":
    main()
