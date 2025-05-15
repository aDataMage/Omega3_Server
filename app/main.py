from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import (
    # customer_router,
    # order_router,
    # order_item_router,
    product_router,
    # return_router,
    # store_router,
    kpi_router,
)

app = FastAPI(
    title="My Retail API",
    version="0.1.0",
    description="FastAPI backend for retail KPIs and operations.",
)

# CORS setup (consider loading from env in productio
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ⚠️ Replace with allowed domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["root"])
async def root():
    return {"message": "Retail API up and running"}


# Register routers
# app.include_router(customer_router, prefix="/customers", tags=["Customers"])
# app.include_router(store_router, prefix="/stores", tags=["Stores"])
app.include_router(product_router, prefix="/products", tags=["Products"])
# app.include_router(order_router, prefix="/orders", tags=["Orders"])
# app.include_router(return_router, prefix="/returns", tags=["Returns"])
# app.include_router(order_item_router, prefix="/order-items", tags=["Order Items"])
app.include_router(kpi_router, prefix="/kpi", tags=["kpi, v2"])

# Optional: lifespan event handlers (if needed for DB/session/metrics)
#
#
