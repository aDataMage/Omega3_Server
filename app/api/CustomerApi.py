from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.schemas.CustomerSchema import Customer, CustomerCreate
from app.crud import CustomerCrud as customer_crud
from app.db.session import get_db

router = APIRouter()


@router.get("/", response_model=list[Customer])
def read_customers(db: Session = Depends(get_db)):
    return customer_crud.get_all_customers(db)


@router.post("/", response_model=Customer)
def add_customer(customer: CustomerCreate, db: Session = Depends(get_db)):
    return customer_crud.create_customer(db, customer)


@router.get("/{customer_id}", response_model=Customer)
def read_customer(customer_id: str, db: Session = Depends(get_db)):
    db_customer = customer_crud.get_customer_by_id(db, customer_id)
    if db_customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return db_customer


@router.put("/{customer_id}", response_model=Customer)
def update_customer(
    customer_id: str, customer: CustomerCreate, db: Session = Depends(get_db)
):
    db_customer = customer_crud.update_customer(db, customer_id, customer)
    if db_customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return db_customer


@router.delete("/{customer_id}")
def delete_customer(customer_id: str, db: Session = Depends(get_db)):
    success = customer_crud.delete_customer(db, customer_id)
    if not success:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"detail": "Customer deleted"}
