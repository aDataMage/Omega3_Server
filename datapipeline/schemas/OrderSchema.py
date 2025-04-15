from pydantic import BaseModel
from uuid import UUID
from datetime import datetime, date
from enumsC import (
    OrderStatusEnum,
    PaymentMethodEnum,
    PaymentStatusEnum,
)


class OrderSchema(BaseModel):
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

    class Config:
        from_attributes = True
