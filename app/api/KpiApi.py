from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, logger
from sqlalchemy.orm import Session
from crud import KpiCrud
from db.session import get_db

router = APIRouter()


@router.get("")
def get_all_kpi(
    db: Session = Depends(get_db),
    start_date: str = None,
    end_date: Optional[str] = None,
):
    """
    Get all KPIs for the current month.
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    try:
        kpi_data = KpiCrud.get_all_kpi(db=db, start_date=start_date, end_date=end_date)
        if kpi_data is None:
            raise HTTPException(
                status_code=404, detail="No KPI data found for the given period"
            )
        return kpi_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/insight")
def get_insight(
    comparison_level: str,
    metric: str,
    selected_regions: List[str] = Query(None),
    selected_stores: List[str] = Query(None),
    selected_brands: List[str] = Query(None),
    selected_products: List[str] = Query(None),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="End date in YYYY-MM-DD format"),
    db: Session = Depends(get_db),
):
    try:
        # Parse dates with timezone awareness
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if end_date else datetime.now(timezone.utc)
        
        # Validate date range
        if end_date < start_date:
            raise HTTPException(
                status_code=400,
                detail="End date cannot be before start date"
            )
            
        data = KpiCrud.fetch_insights(
            db=db,
            comparison_level=comparison_level,
            metric=metric,
            selected_regions=selected_regions,
            selected_stores=selected_stores,
            selected_brands=selected_brands,
            selected_products=selected_products,
            start_date=start_date,
            end_date=end_date
        )
        
        if not data:
            raise HTTPException(
                status_code=404,
                detail="No data found for the selected filters and date range"
            )
            
        return {
            "data": data,
            "meta": {
                "comparison_level": comparison_level,
                "metric": metric,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "filter_counts": {
                    "regions": len(selected_regions) if selected_regions else 0,
                    "stores": len(selected_stores) if selected_stores else 0,
                    "brands": len(selected_brands) if selected_brands else 0,
                    "products": len(selected_products) if selected_products else 0
                }
            }
        }
        
    except ValueError as e:
        logger.logger.error(f"ValueError in get_insight: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        # Re-raise HTTPExceptions as they're intentional
        raise
    except Exception as e:
        logger.logger.exception("Unexpected error in get_insight")  # This logs the full traceback
        raise HTTPException(status_code=500, detail="Internal server error")  


@router.get("/total-sales")
def get_total_sales(
    db: Session = Depends(get_db),
    start_date: str = None,
    end_date: Optional[str] = None,
):
    """
    Get total sales for the current month.
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    try:
        total_sales = KpiCrud.get_total_sales(
            db=db, start_date=start_date, end_date=end_date
        )
        if total_sales is None:
            raise HTTPException(
                status_code=404, detail="No sales data found for the given period"
            )
        return {"total_sales": total_sales}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
