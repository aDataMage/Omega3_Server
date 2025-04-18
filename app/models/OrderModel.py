from sqlalchemy import Column, Date, Enum, ForeignKey, Numeric, TIMESTAMP, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from db.base import Base
from enumsC import (
    OrderStatusEnum,
    PaymentMethodEnum,
    PaymentStatusEnum,
)


# ENUM definitions


class Order(Base):
    __tablename__ = "orders"

    order_id = Column(UUID(as_uuid=True), primary_key=True)
    store_id = Column(
        UUID(as_uuid=True),
        ForeignKey("stores.store_id", ondelete="CASCADE"),
        nullable=False,
    )
    customer_id = Column(
        UUID(as_uuid=True), ForeignKey("customers.customer_id"), nullable=False
    )
    total_amount = Column(Numeric(10, 2), nullable=False)
    status = Column(
        Enum(OrderStatusEnum), default=OrderStatusEnum.pending, nullable=False
    )
    order_date = Column(Date)
    payment_method = Column(Enum(PaymentMethodEnum))
    payment_status = Column(Enum(PaymentStatusEnum))
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships (optional, define only if using ORM joins)
    store = relationship("Store", back_populates="orders")
    customer = relationship("Customer", back_populates="orders")
    order_items = relationship("OrderItem", back_populates="order")
