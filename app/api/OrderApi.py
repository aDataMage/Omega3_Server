from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.schemas.OrderSchema import Order, OrderCreate, OrderUpdate
from app.crud import OrderCrud
from app.db.session import get_db

router = APIRouter()


@router.post("/", response_model=Order)
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    return order.create_order(db=db, order=order)


@router.get("/", response_model=list[Order])
def get_orders(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    orders = OrderCrud.get_orders(db=db, skip=skip, limit=limit)
    return orders


@router.get("/{order_id}", response_model=Order)
def get_order(order_id: str, db: Session = Depends(get_db)):
    db_order = OrderCrud.get_order_by_id(db=db, order_id=order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order


@router.put("/{order_id}", response_model=Order)
def update_order(order_id: str, order: OrderUpdate, db: Session = Depends(get_db)):
    db_order = order.update_order(db=db, order_id=order_id, order=order)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order


@router.delete("/{order_id}", response_model=Order)
def delete_order(order_id: str, db: Session = Depends(get_db)):
    db_order = OrderCrud.delete_order(db=db, order_id=order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order
