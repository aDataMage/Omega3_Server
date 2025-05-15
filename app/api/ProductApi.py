from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from schemas.ProductSchema import Product
from schemas.ProductSchema import ProductCreate, ProductUpdate
from crud import ProductCrud as product_crud
from db.session import get_db

router = APIRouter()


@router.post("/", response_model=Product)
def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    return product_crud.create_product(db=db, product=product)


@router.get("/", response_model=list[Product])
def get_products(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    products = product_crud.get_products(db=db, skip=skip, limit=limit)
    return products

@router.get("/top")
def get_top_n_products(
    n: int = 10, db: Session = Depends(get_db), metric: str = "Total Sales", start_date: str = None, end_date: str = None 
):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    products = product_crud.get_top_products_by_metric(db=db, metric=metric, start_date=start_date, end_date=end_date, n=n)
    return products

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


@router.delete("/{product_id}", response_model=Product)
def delete_product(product_id: str, db: Session = Depends(get_db)):
    db_product = product_crud.delete_product(db=db, product_id=product_id)
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return db_product

@router.get("/filters/products")
def fetch_product_names(db: Session = Depends(get_db), brands: str = Query(None, alias="selected_brands")):
    selected_brands = brands.split(",") if brands else None
    print("API:", selected_brands)
    return product_crud.get_unique_product_names(db, selected_brands=selected_brands)

@router.get("/filters/brands")
def fetch_brand_names(db: Session = Depends(get_db)):
    return product_crud.get_unique_brand_names(db)

@router.get("/table/brand")
def fetch_store_table(db: Session = Depends(get_db), start_date: str = None, end_date: str = None ):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    return product_crud.get_brand_table_data(db, start_date=start_date, end_date=end_date)


@router.get("/table/product")
def fetch_store_table(db: Session = Depends(get_db), start_date: str = None, end_date: str = None ):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    return product_crud.get_product_table_data(db, start_date=start_date, end_date=end_date)


