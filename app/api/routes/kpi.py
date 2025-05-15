from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session  # Import query_params from fastapi_utils
from helpers import parse_date
from crud.v2.kpi import KPICrud
from crud.kpi.insights import fetch_insights
from db.session import get_db
from fastapi.logger import logger

router = APIRouter()
kpi_crud = KPICrud()


@router.get("/")
def get_all_kpi(
    db: Session = Depends(get_db),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
):
    """
    Get all KPIs for the given date range.
    """
    # Parse dates with timezone awareness
    start_date = parse_date.parse_date_safe(start_date).replace(tzinfo=timezone.utc)
    end_date = (
        parse_date.parse_date_safe(end_date).replace(tzinfo=timezone.utc)
        if end_date
        else datetime.now(timezone.utc)
    )

    # Validate date range
    if end_date < start_date:
        raise HTTPException(
            status_code=400, detail="End date cannot be before start date"
        )
    try:
        kpi_data = kpi_crud.get_all_kpi(db=db, start_date=start_date, end_date=end_date)
        if kpi_data is None:
            raise HTTPException(status_code=404, detail="No KPI data found")
        return kpi_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insight")
def get_insight(
    comparison_level: str = Query("Region", description="Level of comparison"),
    metric: str = Query("Total Sales", description="Metric to compare"),
    selected_regions: List[str] = Query([]),
    selected_stores: List[str] = Query([]),
    selected_brands: List[str] = Query([]),
    selected_products: List[str] = Query([]),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="End date in YYYY-MM-DD format"),
    db: Session = Depends(get_db),
):
    region_list = selected_regions or []
    store_list = selected_stores or []
    brand_list = selected_brands or []
    product_list = selected_products or []
    try:
        # Parse dates with timezone awareness
        start_date = parse_date.parse_date_safe(start_date).replace(tzinfo=timezone.utc)
        end_date = (
            parse_date.parse_date_safe(end_date).replace(tzinfo=timezone.utc)
            if end_date
            else datetime.now(timezone.utc)
        )

        # Validate date range
        if end_date < start_date:
            raise HTTPException(
                status_code=400, detail="End date cannot be before start date"
            )

        comparison_level = comparison_level.lower()
        data = fetch_insights(
            db=db,
            comparison_level=comparison_level,
            metric=metric,
            selected_regions=region_list,
            selected_stores=store_list,
            selected_brands=brand_list,
            selected_products=product_list,
            start_date=start_date,
            end_date=end_date,
        )

        if not data:
            raise HTTPException(
                status_code=404, detail="No data found for the selected filters"
            )

        return {
            "data": data,
            "meta": {
                "comparison_level": comparison_level,
                "metric": metric,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "filter_counts": {
                    "regions": len(selected_regions),
                    "stores": len(selected_stores),
                    "brands": len(selected_brands),
                    "products": len(selected_products),
                },
            },
        }
    except ValueError as e:
        logger.error(f"ValueError in get_insight: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise  # Re-raise HTTPExceptions as they're intentional
    except Exception as e:
        logger.exception("Unexpected error in get_insight")
        raise HTTPException(status_code=500, detail="Internal server error")
