from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from enumsC import RegionEnum


class StoreSchema(BaseModel):
    store_id: UUID
    manager_name: str
    name: str
    is_active: bool
    region: RegionEnum
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
        use_enum_values = True
