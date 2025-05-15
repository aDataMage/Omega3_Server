from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from schemas.CustomerSchema import Customer, CustomerCreate, CustomerUpdate
from crud.v2.customer import CustomerCrud
from db.session import get_db

router = APIRouter()
customer_crud = CustomerCrud()


@router.get("/", response_model=List[Customer])
def read_customers(db: Session = Depends(get_db)):
    return customer_crud.get_all_customers(db)


@router.post("/", response_model=Customer, status_code=201)
def add_customer(customer: CustomerCreate, db: Session = Depends(get_db)):
    return customer_crud.create_customer(db, customer)


@router.get("/{customer_id}", response_model=Customer)
def read_customer(customer_id: str, db: Session = Depends(get_db)):
    customer = customer_crud.get_customer_by_id(db, customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


@router.put("/{customer_id}", response_model=Customer)
def update_customer_route(
    customer_id: str,
    customer_update: CustomerUpdate,
    db: Session = Depends(get_db),
):
    customer = customer_crud.update_customer(db, customer_id, customer_update)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


@router.delete("/{customer_id}", status_code=204)
def delete_customer_route(customer_id: str, db: Session = Depends(get_db)):
    if not customer_crud.delete_customer(db, customer_id):
        raise HTTPException(status_code=404, detail="Customer not found")
    return
