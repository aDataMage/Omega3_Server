from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.crud import KpiCrud
from app.db.session import get_db

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
