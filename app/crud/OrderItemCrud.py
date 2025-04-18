from sqlalchemy.orm import Session
from models.OrderItemsModel import OrderItem
from schemas.OrderItemsSchema import OrderItemCreate, OrderItemUpdate

# Create a new order item


def create_order_item(db: Session, order_item: OrderItemCreate):
    db_order_item = OrderItem(
        order_id=order_item.order_id,
        product_id=order_item.product_id,
        quantity=order_item.quantity,
        discount_applied=order_item.discount_applied,
        price=order_item.price,
        total_price=order_item.total_price,
        store_id=order_item.store_id,
    )
    db.add(db_order_item)
    db.commit()
    db.refresh(db_order_item)
    return db_order_item


# Get all order items


def get_order_items(db: Session, skip: int = 0, limit: int = 100):
    return db.query(OrderItem).offset(skip).limit(limit).all()


# Get order item by ID


def get_order_item_by_id(db: Session, order_item_id: str):
    return db.query(OrderItem).filter(OrderItem.order_item_id == order_item_id).first()


# Update an order item


def update_order_item(db: Session, order_item_id: str, order_item: OrderItemUpdate):
    db_order_item = (
        db.query(OrderItem).filter(OrderItem.order_item_id == order_item_id).first()
    )
    if db_order_item:
        for key, value in order_item.dict(exclude_unset=True).items():
            setattr(db_order_item, key, value)
        db.commit()
        db.refresh(db_order_item)
    return db_order_item


# Delete an order item


def delete_order_item(db: Session, order_item_id: str):
    db_order_item = (
        db.query(OrderItem).filter(OrderItem.order_item_id == order_item_id).first()
    )
    if db_order_item:
        db.delete(db_order_item)
        db.commit()
    return db_order_item
