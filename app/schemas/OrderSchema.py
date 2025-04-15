from typing import Optional
from pydantic import BaseModel
from uuid import UUID
from datetime import datetime, date
from app.enumsC import (
    OrderStatusEnum,
    PaymentMethodEnum,
    PaymentStatusEnum,
)


class OrderBase(BaseModel):
    order_id: UUID
    store_id: UUID
    customer_id: UUID
    total_amount: float
    status: OrderStatusEnum
    order_date: date
    payment_method: PaymentMethodEnum
    payment_status: PaymentStatusEnum
    created_at: datetime
    updated_at: datetime


class OrderCreate(OrderBase):
    pass
    # Inherits from OrderBase, no additional fields needed for creation


class Order(OrderBase):
    order_id: UUID

    class Config:
        from_attributes = True
        use_enum_values = True


class OrderUpdate(OrderBase):
    total_amount: Optional[float] = None
    payment_method: Optional[PaymentMethodEnum] = None
    status: Optional[OrderStatusEnum] = None
    payment_status: Optional[PaymentStatusEnum] = None
    updated_at: Optional[datetime] = None
