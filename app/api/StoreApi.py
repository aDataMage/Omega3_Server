from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from schemas.StoreSchema import Store
from schemas.StoreSchema import StoreCreate, StoreUpdate
from crud import StoreCrud as store_crud
from db.session import get_db


router = APIRouter()


@router.get("/", response_model=list[Store])
def read_stores(db: Session = Depends(get_db)):
    return store_crud.get_all_stores(db)


@router.post("/", response_model=Store)
def add_store(store: StoreCreate, db: Session = Depends(get_db)):
    return store_crud.create_store(db, store)

@router.get("/top")
def get_top_n_stores(
    n: int = 10, db: Session = Depends(get_db), metric: str = "Total Sales", start_date: str = None, end_date: str = None 
):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    stores = store_crud.get_top_stores_by_metric(db=db, metric=metric, start_date=start_date, end_date=end_date, n=n)
    return stores

@router.get("/{store_id}", response_model=Store)
def read_store(store_id: str, db: Session = Depends(get_db)):
    db_store = store_crud.get_store_by_id(db, store_id)
    if db_store is None:
        raise HTTPException(status_code=404, detail="Store not found")
    return db_store


@router.put("/{store_id}", response_model=Store)
def update_store(store_id: str, store: StoreUpdate, db: Session = Depends(get_db)):
    db_store = store_crud.update_store(db, store_id, store)
    if db_store is None:
        raise HTTPException(status_code=404, detail="Store not found")
    return db_store


@router.delete("/{store_id}")
def delete_store(store_id: str, db: Session = Depends(get_db)):
    success = store_crud.delete_store(db, store_id)
    if not success:
        raise HTTPException(status_code=404, detail="Store not found")
    return {"detail": "Store deleted"}


@router.get("/name/{store_name}", response_model=Store)
def read_store_by_name(store_name: str, db: Session = Depends(get_db)):
    db_store = store_crud.get_store_by_name(db, store_name)
    if db_store is None:
        raise HTTPException(status_code=404, detail="Store not found")
    return db_store
