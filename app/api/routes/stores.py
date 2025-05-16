from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from schemas.StoreSchema import Store, StoreCreate, StoreUpdate
from crud.v2.stores import StoreCrud
from schemas.StoreSchema import Store as StoreResponse
from db.session import get_db
from helpers.parse_date import parse_date_safe

router = APIRouter()
store_crud = StoreCrud()
# --- Routes --- #


@router.get("/", response_model=List[Store])
def read_stores(db: Session = Depends(get_db)):
    """Get all stores."""
    return store_crud.get_stores(db)


@router.post("/", response_model=Store)
def add_store(store: StoreCreate, db: Session = Depends(get_db)):
    """Add a new store."""
    return store_crud.create_store(db=db, store=store)


@router.get("/table", response_model=List[dict])
def fetch_table_data(
    db: Session = Depends(get_db),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    group_by: str = Query("store", enum=["store", "region"]),
):
    """Fetch aggregated data by store or region."""
    start_date = parse_date_safe(start_date)
    end_date = parse_date_safe(end_date) if end_date else None

    if group_by == "region":
        return store_crud.get_region_table_data(
            db=db, start_date=start_date, end_date=end_date
        )
    return store_crud.get_store_table_data(
        db=db, start_date=start_date, end_date=end_date
    )


@router.get("/top", response_model=List[dict])
def get_top_n_stores(
    n: int = 10,
    db: Session = Depends(get_db),
    metric: str = "Total Sales",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """Get top N stores by a specific metric."""
    start_date = parse_date_safe(start_date)
    end_date = parse_date_safe(end_date) if end_date else None
    return store_crud.get_top_stores_by_metric(
        db=db, metric=metric, start_date=start_date, end_date=end_date, n=n
    )


@router.get("/{store_id}", response_model=Store)
def read_store(store_id: str, db: Session = Depends(get_db)):
    """Get store by ID."""
    db_store = store_crud.get_store_by_id(db=db, store_id=store_id)
    if db_store is None:
        raise HTTPException(status_code=404, detail="Store not found")
    return db_store


@router.put("/{store_id}", response_model=Store)
def update_store(store_id: str, store: StoreUpdate, db: Session = Depends(get_db)):
    """Update a store."""
    db_store = store_crud.update_store(db=db, store_id=store_id, store=store)
    if db_store is None:
        raise HTTPException(status_code=404, detail="Store not found")
    return db_store


@router.delete("/{store_id}")
def delete_store(store_id: str, db: Session = Depends(get_db)):
    """Delete a store."""
    success = store_crud.delete_store(db=db, store_id=store_id)
    if not success:
        raise HTTPException(status_code=404, detail="Store not found")
    return {"detail": "Store deleted"}


@router.get("/name/{store_name}", response_model=Store)
def read_store_by_name(store_name: str, db: Session = Depends(get_db)):
    """Get store by name."""
    db_store = store_crud.get_store_by_name(db=db, store_name=store_name)
    if db_store is None:
        raise HTTPException(status_code=404, detail="Store not found")
    return db_store


@router.get("/filters/regions", response_model=List[str])
def fetch_regions(db: Session = Depends(get_db)):
    """Fetch unique store regions."""
    return store_crud.get_unique_regions(db)


@router.get("/filters/stores", response_model=List[dict])
def fetch_stores(
    db: Session = Depends(get_db),
    regions: str = Query(None, alias="selected_regions"),
):
    """Fetch store names with optional region filtering."""
    selected_regions = regions.split(",") if regions else None
    return store_crud.get_unique_store_names(db=db, selected_regions=selected_regions)
