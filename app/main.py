from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api import (
    CustomerApi,
    OrderApi,
    OrderItemApi,
    ProductApi,
    ReturnsApi,
    StoreApi,
    KpiApi,
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    # Or ["*"] to allow all (not recommended in prod)
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "Hello World"}


app.include_router(CustomerApi.router, prefix="/customers", tags=["customers"])
app.include_router(StoreApi.router, prefix="/stores", tags=["stores"])
app.include_router(ProductApi.router, prefix="/products", tags=["products"])
app.include_router(OrderApi.router, prefix="/orders", tags=["orders"])
app.include_router(ReturnsApi.router, prefix="/returns", tags=["returns"])
app.include_router(OrderItemApi.router, prefix="/order-items", tags=["order_items"])
app.include_router(KpiApi.router, prefix="/kpi", tags=["kpi"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)