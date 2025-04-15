from sqlalchemy.orm import Session
from app.models.ReturnsModel import Return
from app.schemas.ReturnsSchema import ReturnCreate, ReturnUpdate

# Create a new return


def create_return(db: Session, return_: ReturnCreate):
    db_return = Return(
        order_id=return_.order_id,
        product_id=return_.product_id,
        return_date=return_.return_date,
        return_reason=return_.return_reason,
        refund_amount=return_.refund_amount,
        return_status=return_.return_status,
        store_id=return_.store_id,
    )
    db.add(db_return)
    db.commit()
    db.refresh(db_return)
    return db_return


# Get all returns


def get_returns(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Return).offset(skip).limit(limit).all()


# Get return by ID


def get_return_by_id(db: Session, return_id: str):
    return db.query(Return).filter(Return.return_id == return_id).first()


# Update a return


def update_return(db: Session, return_id: str, return_: ReturnUpdate):
    db_return = db.query(Return).filter(Return.return_id == return_id).first()
    if db_return:
        for key, value in return_.dict(exclude_unset=True).items():
            setattr(db_return, key, value)
        db.commit()
        db.refresh(db_return)
    return db_return


# Delete a return


def delete_return(db: Session, return_id: str):
    db_return = db.query(Return).filter(Return.return_id == return_id).first()
    if db_return:
        db.delete(db_return)
        db.commit()
    return db_return
