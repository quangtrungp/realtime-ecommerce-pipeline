
import random
from uuid import uuid4
from decimal import Decimal
import os,sys


from model_generator.order import (
    create_order_created_event,
    create_order_cancelled_event,
    create_order_completed_event
)
from model_generator.common import get_current_timestamp


class OrderGenerator:

    def __init__(self):
        self.orders = {}

        self.products = [
            {
                "product_id": "prd_001",
                "unit_price": Decimal("500000.00")
            },
            {
                "product_id": "prd_002",
                "unit_price": Decimal("300000.00")
            },
            {
                "product_id": "prd_003",
                "unit_price": Decimal("150000.00")
            },
            {
                "product_id": "prd_004",
                "unit_price": Decimal("800000.00")
            },
            {
                "product_id": "prd_005",
                "unit_price": Decimal("1200000.00")
            }
        ]

    def generate_items(self):
        selected_products = random.sample(
            self.products,
            k=random.randint(1, 3)
        )

        items = []

        for product in selected_products:
            items.append({
                "product_id": product["product_id"],
                "quantity": random.randint(1, 3),
                "unit_price": product["unit_price"]
            })

        return items

    def generate_order(self):

        order_id = f"ord_{uuid4().hex[:12]}"
        user_id = random.randint(1000, 9999)
        items = self.generate_items()

        event = create_order_created_event(
            order_id=order_id,
            user_id=user_id,
            items=items
        )

        payload = event["payload"]

        # Save business state
        self.orders[order_id] = {
            "order_id": order_id,
            "user_id": user_id,
            "items": items,
            "total_amount": Decimal(
                payload["total_amount"]
            ),
            "currency": payload["currency"],
            "status": "created",
            "payment_attempts": []
        }

        return event

    
    def generate_order_final_event(self, order_id, final_status):

        order = self.orders[order_id]

        if order["status"] != "created":
            return None

        if final_status == "completed":

            completed_at = get_current_timestamp()

            event = create_order_completed_event(
                order_id=order_id,
                completed_at=completed_at
            )

        elif final_status == "cancelled":

            event = create_order_cancelled_event(
                order_id=order_id,
                reason="payment_timeout",
                cancelled_by="system"
            )

        else:
            raise ValueError("Invalid order final status")

        order["status"] = final_status

        return event
