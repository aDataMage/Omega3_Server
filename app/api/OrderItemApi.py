from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.schemas.OrderItemsSchema import (
    OrderItem,
    OrderItemCreate,
    OrderItemUpdate,
)
from app.crud import OrderItemCrud as order_item_crud
from app.db.session import get_db

router = APIRouter()


@router.post("/", response_model=OrderItem)
def create_order_item(order_item: OrderItemCreate, db: Session = Depends(get_db)):
    return order_item_crud.create_order_item(db=db, order_item=order_item)


@router.get("/", response_model=list[OrderItem])
def get_order_items(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    order_items = order_item_crud.get_order_items(db=db, skip=skip, limit=limit)
    return order_items


@router.get("/{order_item_id}", response_model=OrderItem)
def get_order_item(order_item_id: str, db: Session = Depends(get_db)):
    db_order_item = order_item_crud.get_order_item_by_id(
        db=db, order_item_id=order_item_id
    )
    if db_order_item is None:
        raise HTTPException(status_code=404, detail="Order item not found")
    return db_order_item


@router.put("/{order_item_id}", response_model=OrderItem)
def update_order_item(
    order_item_id: str, order_item: OrderItemUpdate, db: Session = Depends(get_db)
):
    db_order_item = order_item_crud.update_order_item(
        db=db, order_item_id=order_item_id, order_item=order_item
    )
    if db_order_item is None:
        raise HTTPException(status_code=404, detail="Order item not found")
    return db_order_item


@router.delete("/{order_item_id}", response_model=OrderItem)
def delete_order_item(order_item_id: str, db: Session = Depends(get_db)):
    db_order_item = order_item_crud.delete_order_item(
        db=db, order_item_id=order_item_id
    )
    if db_order_item is None:
        raise HTTPException(status_code=404, detail="Order item not found")
    return db_order_item
