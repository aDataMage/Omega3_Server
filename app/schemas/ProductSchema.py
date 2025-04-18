from typing import Optional
from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from enumsC import (
    BrandEnum,
    CategoryEnum,
)


class ProductBase(BaseModel):
    name: str
    price: float
    cost: float
    brand: BrandEnum
    category: CategoryEnum
    stock_quantity: int


class ProductCreate(ProductBase):
    pass


class Product(ProductBase):
    product_id: UUID
    created_at: datetime
    updated_at: datetime


class ProductUpdate(ProductBase):
    product_name: Optional[str] = None
    category: Optional[str] = None
    brand: Optional[str] = None
    price: Optional[float] = None
    cost: Optional[float] = None
    stock_quantity: Optional[int] = None

    # All fields are optional for update operations
    class Config:
        from_attributes = True
