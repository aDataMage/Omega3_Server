from sqlalchemy.orm import Session
from models.StoreModel import Store
from schemas.StoreSchema import StoreCreate, StoreUpdate
import uuid


def get_all_stores(db: Session):
    return db.query(Store).all()


def create_store(db: Session, store: StoreCreate):
    db_store = Store(
        store_id=str(uuid.uuid4()),
        store_name=store.store_name,
        region=store.region,
        manager_name=store.manager_name,
        opening_date=store.opening_date,
    )
    db.add(db_store)
    db.commit()
    db.refresh(db_store)
    return db_store


def get_store_by_id(db: Session, store_id: str):
    return db.query(Store).filter(Store.store_id == store_id).first()


def update_store(db: Session, store_id: str, store: StoreUpdate):
    db_store = db.query(Store).filter(Store.store_id == store_id).first()
    if db_store:
        if store.store_name:
            db_store.store_name = store.store_name
        if store.region:
            db_store.region = store.region
        if store.manager_name:
            db_store.manager_name = store.manager_name
        if store.opening_date:
            db_store.opening_date = store.opening_date
        db.commit()
        db.refresh(db_store)
    return db_store


def delete_store(db: Session, store_id: str):
    db_store = db.query(Store).filter(Store.store_id == store_id).first()
    if db_store:
        db.delete(db_store)
        db.commit()
        return True
    return False


def get_store_by_name(db: Session, store_name: str):
    return db.query(Store).filter(Store.store_name == store_name).first()
