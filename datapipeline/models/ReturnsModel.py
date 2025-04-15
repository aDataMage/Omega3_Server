from sqlalchemy import Column, ForeignKey, Date, Numeric, Text, Enum, TIMESTAMP, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid

from ..database import Base
from ..utils.enums import ReturnStatusEnum


class Return(Base):
    __tablename__ = "returns"

    return_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_item_id = Column(
        UUID(as_uuid=True),
        ForeignKey("order_items.order_item_id", ondelete="CASCADE"),
        nullable=False,
    )

    reason = Column(Text)
    return_date = Column(Date)
    refund_amount = Column(Numeric(10, 2))
    return_status = Column(Enum(ReturnStatusEnum), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    # Relationships (optional)
    order_item = relationship("OrderItem", back_populates="returns")
