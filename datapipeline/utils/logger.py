import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Union
from sqlalchemy import create_engine


class DataQualityLogger:
    """
    A reusable module for logging DataFrame statistics with monthly partitioning.
    """

    def __init__(
        self, log_method: str = "file", log_path: str = None, db_uri: str = None
    ):
        """
        Initialize the logger.

        Args:
            log_method: 'file' or 'database'
            log_path: Required for file logging
            db_uri: Database connection string (e.g., 'postgresql://user:pass@host/db')
        """
        self.log_method = log_method
        self.log_path = Path(log_path) if log_path else None
        self.db_uri = db_uri

        if log_method == "file" and not log_path:
            raise ValueError("log_path is required for file logging")
        if log_method == "database" and not db_uri:
            raise ValueError("db_uri is required for database logging")

    def log_stats(
        self, df: pd.DataFrame, table_name: str
    ) -> Dict[str, Union[str, int, Dict]]:
        """
        Log DataFrame statistics with monthly timestamp.

        Args:
            df: DataFrame to analyze
            table_name: Name of the table/entity being processed

        Returns:
            Dictionary containing all logged statistics
        """
        # Get current month/year
        month_year = datetime.now().strftime("%Y-%m")
        timestamp = datetime.now().isoformat()

        # Calculate statistics
        stats = {
            "timestamp": timestamp,
            "month_year": month_year,
            "table_name": table_name,
            "row_count": len(df),
            "columns": list(df.columns),
            "null_counts": df.isnull().sum().to_dict(),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "sample_data": {col: str(df[col].iloc[0]) for col in df.columns},
        }

        if self.log_method == "file":
            self._log_to_file(stats)
        elif self.log_method == "database":
            self._log_to_database(stats)

        return stats

    def _log_to_file(self, stats: Dict):
        """Write logs to monthly partitioned files"""
        log_file = self.log_path / f"data_quality_{stats['month_year']}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        log_entry = self._format_log_entry(stats)

        with open(log_file, "a") as f:
            f.write(log_entry)

    def _log_to_database(self, stats: Dict):
        """Write logs to database table"""
        engine = create_engine(self.db_uri)
        log_df = pd.DataFrame([stats])
        log_df.to_sql("data_quality_logs", engine, if_exists="append", index=False)

    def _format_log_entry(self, stats: Dict) -> str:
        """Format log entry for file output"""
        return f"""
=== {stats["timestamp"]} ===
Table: {stats["table_name"]}
Rows: {stats["row_count"]}
Columns:
{self._format_columns(stats)}
Sample row:
{stats["sample_data"]}
"""

    def _format_columns(self, stats: Dict) -> str:
        """Format column information"""
        return "\n".join(
            f"  - {col} ({stats['dtypes'][col]}): {stats['null_counts'][col]} nulls"
            for col in stats["columns"]
        )
