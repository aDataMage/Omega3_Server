from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import date, datetime
from app.enumsC import ReturnStatusEnum


class ReturnBase(BaseModel):
    order_item_id: UUID
    reason: Optional[str] = None
    return_date: Optional[date] = None
    refund_amount: Optional[float] = None
    return_status: Optional[ReturnStatusEnum] = None


class ReturnCreate(ReturnBase):
    pass
    # Inherits from ReturnBase, no additional fields needed for creation


class Return(ReturnBase):
    return_id: UUID

    class Config:
        orm_mode = True
        # Enable ORM mode to work with SQLAlchemy models


class ReturnUpdate(ReturnBase):
    return_reason: Optional[str] = None
    refund_amount: Optional[float] = None
    return_status: Optional[str] = None
