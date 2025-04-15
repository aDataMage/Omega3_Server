from pydantic import BaseModel
from uuid import UUID
from decimal import Decimal


class OrderItemSchema(BaseModel):
    order_item_id: UUID
    order_id: UUID
    product_id: UUID | None = None
    price: Decimal
    discount_applied: Decimal | None = None
    quantity: int
    total_price: Decimal

    class Config:
        from_attributes = True
