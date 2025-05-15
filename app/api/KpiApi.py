from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, logger
from sqlalchemy.orm import Session
from crud.kpi.keyMetrics import get_all_kpi
from crud.kpi.otherMetrics import (
    fetch_customer_metric_trend,
    fetch_customer_metrics,
    fetch_segmented_customer_metric,
)
from crud.kpi.insights import fetch_insights
from db.session import get_db

router = APIRouter()


@router.get("")
def getAllKpi(
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
        kpi_data = get_all_kpi(db=db, start_date=start_date, end_date=end_date)
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
    region_list = selected_regions or []
    store_list = selected_stores or []
    brand_list = selected_brands or []
    product_list = selected_products or []

    loging = logger.logger

    loging.debug(region_list)

    try:
        # Parse dates with timezone awareness
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if end_date
            else datetime.now(timezone.utc)
        )

        # Validate date range
        if end_date < start_date:
            raise HTTPException(
                status_code=400, detail="End date cannot be before start date"
            )

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
                status_code=404,
                detail="No data found for the selected filters and date range",
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
                    "products": len(selected_products) if selected_products else 0,
                },
            },
        }

    except ValueError as e:
        logger.logger.error(f"ValueError in get_insight: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        # Re-raise HTTPExceptions as they're intentional
        raise
    except Exception as e:
        logger.logger.exception(
            "Unexpected error in get_insight"
        )  # This logs the full traceback
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/customer_metrics")
def get_customer_metrics(
    comparison_level: str,
    selected_regions: List[str] = Query(None),
    selected_stores: List[str] = Query(None),
    selected_brands: List[str] = Query(None),
    selected_products: List[str] = Query(None),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="End date in YYYY-MM-DD format"),
    db: Session = Depends(get_db),
):
    """
    Get all customer metrics (Total Customers, New Customers, ARPC, Repeat Rate)
    with comparison breakdown by the specified level (region/store/brand/product).
    """

    print("called")
    region_list = selected_regions or []
    store_list = selected_stores or []
    brand_list = selected_brands or []
    product_list = selected_products or []

    logger.logger.debug(
        f"Fetching customer metrics with filters - regions: {region_list}, stores: {store_list}"
    )

    try:
        # Parse dates with timezone awareness
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if end_date
            else datetime.now(timezone.utc)
        )

        # Validate date range
        if end_date < start_date:
            raise HTTPException(
                status_code=400, detail="End date cannot be before start date"
            )

        metrics_data = fetch_customer_metrics(
            db=db,
            comparison_level=comparison_level,
            selected_regions=region_list,
            selected_stores=store_list,
            selected_brands=brand_list,
            selected_products=product_list,
            start_date=start_date,
            end_date=end_date,
        )

        if not metrics_data:
            raise HTTPException(
                status_code=404,
                detail="No data found for the selected filters and date range",
            )

        return {
            "data": metrics_data,
            "meta": {
                "comparison_level": comparison_level,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "filter_counts": {
                    "regions": len(region_list),
                    "stores": len(store_list),
                    "brands": len(brand_list),
                    "products": len(product_list),
                },
                "metrics_returned": [m["metric_name"] for m in metrics_data],
            },
        }
    except ValueError as e:
        logger.logger.error(f"ValueError in get_customer_metrics: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        # Re-raise HTTPExceptions as they're intentional
        raise
    except Exception as e:
        logger.logger.exception("Unexpected error in get_customer_metrics")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/customer_trend")
def get_customer_trend(
    comparison_level: str,
    metric_name: str = Query(..., alias="metric_name"),
    selected_regions: List[str] = Query(None),
    selected_stores: List[str] = Query(None),
    selected_brands: List[str] = Query(None),
    selected_products: List[str] = Query(None),
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
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if end_date
            else datetime.now(timezone.utc)
        )

        # Validate date range
        if end_date < start_date:
            raise HTTPException(
                status_code=400, detail="End date cannot be before start date"
            )

        metrics_trend_data = fetch_customer_metric_trend(
            db=db,
            metric_name=metric_name,
            comparison_level=comparison_level,
            selected_regions=region_list,
            selected_stores=store_list,
            selected_brands=brand_list,
            selected_products=product_list,
            start_date=start_date,
            end_date=end_date,
        )

        if not metrics_trend_data:
            raise HTTPException(
                status_code=404,
                detail="No data found for the selected filters and date range",
            )

        return {
            "data": metrics_trend_data,
            "meta": {
                "comparison_level": comparison_level,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "filter_counts": {
                    "regions": len(region_list),
                    "stores": len(store_list),
                    "brands": len(brand_list),
                    "products": len(product_list),
                },
            },
        }
    except ValueError as e:
        logger.logger.error(f"ValueError in get_customer_metrics: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        # Re-raise HTTPExceptions as they're intentional
        raise
    except Exception as e:
        logger.logger.exception("Unexpected error in get_customer_metrics")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/customer_segment")
def get_customer_segment(
    comparison_level: str,
    metric_name: str = Query(..., alias="metric_name"),
    segment_name: str = Query(..., alias="segment_name"),
    selected_regions: List[str] = Query(None),
    selected_stores: List[str] = Query(None),
    selected_brands: List[str] = Query(None),
    selected_products: List[str] = Query(None),
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
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if end_date
            else datetime.now(timezone.utc)
        )

        # Validate date range
        if end_date < start_date:
            raise HTTPException(
                status_code=400, detail="End date cannot be before start date"
            )

        metrics_trend_data = fetch_segmented_customer_metric(
            db=db,
            metric_name=metric_name,
            segment_by=segment_name,
            comparison_level=comparison_level,
            selected_regions=region_list,
            selected_stores=store_list,
            selected_brands=brand_list,
            selected_products=product_list,
            start_date=start_date,
            end_date=end_date,
        )

        if not metrics_trend_data:
            raise HTTPException(
                status_code=404,
                detail="No data found for the selected filters and date range",
            )

        return {
            "data": metrics_trend_data,
            "meta": {
                "comparison_level": comparison_level,
                "segment_name": segment_name,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "filter_counts": {
                    "regions": len(region_list),
                    "stores": len(store_list),
                    "brands": len(brand_list),
                    "products": len(product_list),
                },
            },
        }
    except ValueError as e:
        logger.logger.error(f"ValueError in get_customer_metrics: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        # Re-raise HTTPExceptions as they're intentional
        raise
    except Exception as e:
        logger.logger.exception("Unexpected error in get_customer_metrics")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/customer_info")
def get_customer_info(
    comparison_level: str,
    metric_name: str = Query(..., alias="metric_name"),
    segment_name: Optional[str] = Query(None, alias="segment_name"),
    return_trend: Optional[bool] = Query(None, alias="return_trend"),
    selected_regions: List[str] = Query(None),
    selected_stores: List[str] = Query(None),
    selected_brands: List[str] = Query(None),
    selected_products: List[str] = Query(None),
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
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if end_date
            else datetime.now(timezone.utc)
        )

        # Validate date range
        if end_date < start_date:
            raise HTTPException(
                status_code=400, detail="End date cannot be before start date"
            )

        metrics_trend_data = KpiCrud.fetch_customer_info(
            db=db,
            metric_name=metric_name,
            return_trend=return_trend,
            segment_by=segment_name,
            comparison_level=comparison_level,
            selected_regions=region_list,
            selected_stores=store_list,
            selected_brands=brand_list,
            selected_products=product_list,
            start_date=start_date,
            end_date=end_date,
        )

        if not metrics_trend_data:
            raise HTTPException(
                status_code=404,
                detail="No data found for the selected filters and date range",
            )

        return {
            "data": metrics_trend_data,
            "meta": {
                "comparison_level": comparison_level,
                "segment_name": segment_name,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "filter_counts": {
                    "regions": len(region_list),
                    "stores": len(store_list),
                    "brands": len(brand_list),
                    "products": len(product_list),
                },
            },
        }
    except ValueError as e:
        logger.logger.error(f"ValueError in get_customer_metrics: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        # Re-raise HTTPExceptions as they're intentional
        raise
    except Exception as e:
        logger.logger.exception("Unexpected error in get_customer_metrics")
        raise HTTPException(status_code=500, detail="Internal server error")
