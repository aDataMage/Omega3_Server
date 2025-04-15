from sqlalchemy.orm import Session
from app.models.OrderModel import Order
from app.schemas.OrderSchema import OrderCreate, OrderUpdate

# Create a new order


def create_order(db: Session, order: OrderCreate):
    db_order = Order(
        customer_id=order.customer_id,
        store_id=order.store_id,
        order_date=order.order_date,
        total_amount=order.total_amount,
        payment_method=order.payment_method,
        order_status=order.order_status,
    )
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    return db_order


# Get all orders


def get_orders(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Order).offset(skip).limit(limit).all()


# Get order by ID


def get_order_by_id(db: Session, order_id: str):
    return db.query(Order).filter(Order.order_id == order_id).first()


# Update an order


def update_order(db: Session, order_id: str, order: OrderUpdate):
    db_order = db.query(Order).filter(Order.order_id == order_id).first()
    if db_order:
        for key, value in order.dict(exclude_unset=True).items():
            setattr(db_order, key, value)
        db.commit()
        db.refresh(db_order)
    return db_order


# Delete an order


def delete_order(db: Session, order_id: str):
    db_order = db.query(Order).filter(Order.order_id == order_id).first()
    if db_order:
        db.delete(db_order)
        db.commit()
    return db_order
