from sqlalchemy.orm import Session
from app.models.CustomersModel import Customer
from app.schemas.CustomerSchema import CustomerCreate
import uuid


def get_all_customers(db: Session):
    return db.query(Customer).all()


def create_customer(db: Session, customer: CustomerCreate):
    db_customer = Customer(
        customer_id=str(uuid.uuid4()), **customer.model_dump()
    )  # Auto-generate UUID
    db.add(db_customer)
    db.commit()
    db.refresh(db_customer)
    return db_customer


def get_customer_by_id(db: Session, customer_id: str):
    return db.query(Customer).filter(Customer.customer_id == customer_id).first()


def update_customer(db: Session, customer_id: str, customer: CustomerCreate):
    db_customer = db.query(Customer).filter(Customer.customer_id == customer_id).first()
    if db_customer:
        for key, value in customer.dict().items():
            setattr(db_customer, key, value)
        db.commit()
        db.refresh(db_customer)
        return db_customer
    return None


def delete_customer(db: Session, customer_id: str):
    db_customer = db.query(Customer).filter(Customer.customer_id == customer_id).first()
    if db_customer:
        db.delete(db_customer)
        db.commit()
        return True
    return False
