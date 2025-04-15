from typing import Optional
from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from app.enumsC import RegionEnum


class StoreBase(BaseModel):
    store_id: UUID
    manager_name: str
    name: str
    is_active: bool
    region: RegionEnum
    created_at: datetime
    updated_at: datetime


class StoreCreate(StoreBase):
    pass
    # Inherits from StoreBase, no additional fields needed for creation


class Store(StoreBase):
    store_id: UUID
    # Overriding store_id to be non-optional for the response model

    class Config:
        from_attributes = True
        use_enum_values = True
        json_encoders = {
            UUID: lambda v: str(v),  # Convert UUID to string for JSON
        }


class StoreUpdate(StoreBase):
    name: Optional[str] = None
    region: Optional[str] = None
    manager_name: Optional[str] = None
    is_active: Optional[bool] = None
    updated_at: Optional[datetime] = None
    # All fields are optional for update operations
