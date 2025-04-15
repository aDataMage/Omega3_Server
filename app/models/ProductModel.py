from sqlalchemy import (
    Column,
    String,
    Integer,
    Enum,
    DateTime,
    NUMERIC,
)
from uuid import uuid4
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, timezone
from app.db.base import Base
from app.enumsC import (
    BrandEnum,
    CategoryEnum,
)
from sqlalchemy.orm import relationship


class Product(Base):
    __tablename__ = "products"

    product_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    name = Column(String, nullable=False)
    price = Column(NUMERIC(10, 2), nullable=False)
    cost = Column(NUMERIC(10, 2))
    brand = Column(Enum(BrandEnum), nullable=False)
    category = Column(Enum(CategoryEnum), nullable=False)
    stock_quantity = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
    )

    order_item = relationship("OrderItem", back_populates="product")
