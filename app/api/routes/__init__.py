from .customers import router as customer_router

# from .orders import router as order_router
# from .order_items import router as order_item_router
from .products import router as product_router

# from .returns import router as return_router
from .stores import router as store_router
from .kpi import router as kpi_router

__all__ = [
    "customer_router",
    "store_router",
    "order_router",
    "product_router",
    "order_item_router",
    "return_router",
    "kpi_router",
]
