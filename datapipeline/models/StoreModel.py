from sqlalchemy import Column, String, Boolean, DateTime, Enum as PgEnum
from sqlalchemy.dialects.postgresql import UUID
from uuid import uuid4
from datetime import datetime
from ..database import Base  # assuming you have a Base class defined here
from ..utils.enums import RegionEnum  # assuming you have a RegionEnum defined here
from sqlalchemy.orm import relationship


class Store(Base):
    __tablename__ = "stores"

    store_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    manager_name = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    is_active = Column(Boolean, default=True)
    region = Column(PgEnum(RegionEnum, name="region"), nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    order = relationship("Order", back_populates="stores")
