from datetime import datetime
from fastapi import HTTPException


def parse_date_safe(date_str: str | None) -> datetime | None:
    if date_str:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid date format. Use YYYY-MM-DD."
            )
    return None
