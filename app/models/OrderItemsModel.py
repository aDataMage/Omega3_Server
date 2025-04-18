from sqlalchemy import Column, ForeignKey, Numeric, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from uuid import uuid4

from db.base import Base  # assuming all models inherit from a common Base


class OrderItem(Base):
    __tablename__ = "order_items"

    order_item_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id = Column(
        UUID(as_uuid=True),
        ForeignKey("orders.order_id", ondelete="CASCADE"),
        nullable=False,
    )
    product_id = Column(
        UUID(as_uuid=True), ForeignKey("products.product_id", ondelete="SET NULL")
    )

    price = Column(Numeric(10, 2), nullable=False)
    discount_applied = Column(Numeric(2, 2))  # e.g., 0.25 = 25% discount
    quantity = Column(Integer, nullable=False)
    total_price = Column(Numeric(10, 2), nullable=False)

    # Optional relationships
    order = relationship("Order", back_populates="order_items")
    product = relationship("Product", back_populates="order_item")
    reverse = relationship(
        "Return", back_populates="order_item", cascade="all, delete-orphan"
    )
