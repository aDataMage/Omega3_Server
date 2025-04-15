from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import date, datetime
from enumsC import ReturnStatusEnum


class ReturnBase(BaseModel):
    order_item_id: UUID
    reason: Optional[str] = None
    return_date: Optional[date] = None
    refund_amount: Optional[float] = None
    return_status: Optional[ReturnStatusEnum] = None


class ReturnCreate(ReturnBase):
    pass


class ReturnOut(ReturnBase):
    return_id: UUID
    created_at: datetime

    class Config:
        orm_mode = True
