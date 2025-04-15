from typing import Optional
from pydantic import BaseModel
from uuid import UUID
from decimal import Decimal


class OrderItemBase(BaseModel):
    order_item_id: UUID
    order_id: UUID
    product_id: UUID | None = None
    price: Decimal
    discount_applied: Decimal | None = None
    quantity: int
    total_price: Decimal


class OrderItemCreate(OrderItemBase):
    pass
    # Inherits from OrderItemBase, no additional fields needed for creation


class OrderItem(OrderItemBase):
    order_item_id: UUID

    class Config:
        orm_mode = True
        # Enable ORM mode to work with SQLAlchemy models


class OrderItemUpdate(OrderItemBase):
    quantity: Optional[int] = None
    discount_applied: Optional[float] = None
    price: Optional[float] = None
    total_price: Optional[float] = None
    # All fields are optional for update operations
