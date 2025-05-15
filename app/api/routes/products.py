from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from schemas.ProductSchema import Product, ProductCreate, ProductUpdate
from crud.v2.product import ProductCrud
from db.session import get_db
from typing import List, Optional
from helpers.parse_date import parse_date_safe

router = APIRouter()
product_crud = ProductCrud()
# --- Routes --- #


@router.post("/", response_model=Product)
def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    return product_crud.create_product(db=db, product=product)


@router.get("/", response_model=List[Product])
def get_products(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return product_crud.get_products(db=db, skip=skip, limit=limit)


@router.get("/top")
def get_top_n_products(
    n: int = 10,
    db: Session = Depends(get_db),
    metric: str = "Total Sales",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    start = parse_date_safe(start_date)
    end = parse_date_safe(end_date)
    return product_crud.get_top_products_by_metric(
        db=db, metric=metric, start_date=start, end_date=end, n=n
    )


@router.get("/{product_id}", response_model=Product)
def get_product(product_id: str, db: Session = Depends(get_db)):
    db_product = product_crud.get_product_by_id(db=db, product_id=product_id)
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return db_product


@router.put("/{product_id}", response_model=Product)
def update_product(
    product_id: str, product: ProductUpdate, db: Session = Depends(get_db)
):
    db_product = product_crud.update_product(
        db=db, product_id=product_id, product=product
    )
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return db_product


@router.delete("/{product_id}")
def delete_product(product_id: str, db: Session = Depends(get_db)):
    success = product_crud.delete_product(db=db, product_id=product_id)
    if not success:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"detail": "Product deleted"}


@router.get("/filters/products")
def fetch_product_names(
    db: Session = Depends(get_db),
    selected_brands: Optional[List[str]] = Query(None),
):
    return product_crud.get_unique_product_names(db, selected_brands=selected_brands)


@router.get("/filters/brands")
def fetch_brand_names(db: Session = Depends(get_db)):
    return product_crud.get_unique_brand_names(db)


@router.get("/table")
def fetch_aggregated_table_data(
    group_by: str = Query("product", enum=["product", "brand"]),
    metric: str = Query("Total Sales"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    sort: Optional[str] = Query("desc", enum=["asc", "desc"]),
    db: Session = Depends(get_db),
):
    start = parse_date_safe(start_date)
    end = parse_date_safe(end_date)

    if group_by == "brand":
        return product_crud.get_brand_table_data(
            db, start_date=start, end_date=end, metric=metric, limit=limit, sort=sort
        )
    else:
        return product_crud.get_product_table_data(
            db, start_date=start, end_date=end, metric=metric, limit=limit, sort=sort
        )
