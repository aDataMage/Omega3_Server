from sqlalchemy.orm import Session
from app.models.ProductModel import Product
from app.schemas.ProductSchema import ProductCreate, ProductUpdate

# Create a new product


def create_product(db: Session, product: ProductCreate):
    db_product = Product(
        product_name=product.product_name,
        category=product.category,
        brand=product.brand,
        price=product.price,
        cost=product.cost,
        stock_quantity=product.stock_quantity,
        store_id=product.store_id,
    )
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product


# Get all products


def get_products(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Product).offset(skip).limit(limit).all()


# Get product by ID


def get_product_by_id(db: Session, product_id: str):
    return db.query(Product).filter(Product.product_id == product_id).first()


# Update a product


def update_product(db: Session, product_id: str, product: ProductUpdate):
    db_product = db.query(Product).filter(Product.product_id == product_id).first()
    if db_product:
        for key, value in product.dict(exclude_unset=True).items():
            setattr(db_product, key, value)
        db.commit()
        db.refresh(db_product)
    return db_product


# Delete a product


def delete_product(db: Session, product_id: str):
    db_product = db.query(Product).filter(Product.product_id == product_id).first()
    if db_product:
        db.delete(db_product)
        db.commit()
    return db_product
