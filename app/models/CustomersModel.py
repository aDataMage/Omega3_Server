import uuid
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Enum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from db.base import Base
from enumsC import (
    RegionEnum,
    MaritalStatusEnum,
    EducationLevelEnum,
    EmploymentStatusEnum,
)
from sqlalchemy.orm import relationship


class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)

    first_name = Column(String(100))
    last_name = Column(String(100))
    age = Column(Integer)
    gender = Column(String(10))
    income_bracket = Column(String(50))
    country = Column(String(100))
    region = Column(Enum(RegionEnum, name="region"))

    phone_number = Column(String(20))
    marital_status = Column(Enum(MaritalStatusEnum, name="marital_status"))
    education_level = Column(Enum(EducationLevelEnum, name="education_level"))
    employment_status = Column(Enum(EmploymentStatusEnum, name="employment_status"))

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    orders = relationship("Order", back_populates="customer")
