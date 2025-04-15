from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime
from uuid import UUID
from enumsC import (
    RegionEnum,
    MaritalStatusEnum,
    EducationLevelEnum,
    EmploymentStatusEnum,
)


# Pydantic model
class CustomerBase(BaseModel):
    email: EmailStr
    password_hash: str
    first_name: Optional[str]
    last_name: Optional[str]
    age: Optional[int]
    gender: Optional[str]
    income_bracket: Optional[str]
    country: Optional[str]
    region: RegionEnum
    phone_number: Optional[str]
    marital_status: MaritalStatusEnum
    education_level: EducationLevelEnum
    employment_status: EmploymentStatusEnum


class CustomerCreate(CustomerBase):
    pass


class CustomerUpdate(CustomerBase):
    updated_at: Optional[datetime]


class Customer(CustomerBase):
    customer_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
