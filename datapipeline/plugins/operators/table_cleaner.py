from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd


class TableCleanOperator(BaseOperator):
    """
    Custom operator to clean and validate tables
    """

    @apply_defaults
    def __init__(self, table_name, input_data, cleaning_rules=None, **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.input_data = input_data
        self.cleaning_rules = cleaning_rules or {}

    def execute(self, context):
        # Get input data
        if self.input_data:
            df = pd.DataFrame(self.input_data)
        else:
            df = pd.read_csv(f"/home/enx/ML/BI_Prod_1/data/raw/{self.table_name}.csv")

        # Clean and validate
        df = self._clean_data(df)
        self._validate(df)

        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Centralized cleaning logic"""
        # General cleaning
        df = df.drop_duplicates().dropna()

        # Capitalize string fields
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip().str.capitalize()

        # Table-specific cleaning
        cleaner = getattr(self, f"clean_{self.table_name}_table", None)
        if cleaner:
            df = cleaner(df)

        return df  # Fixed missing return

    def _validate(self, df: pd.DataFrame):
        """Centralized validation"""
        # Common validation
        id_col = f"{self.table_name.split('_')[0]}_id"
        if id_col in df.columns and df[id_col].isnull().any():
            raise ValueError(f"Null {id_col} values detected")

        # Table-specific validation
        validator = getattr(self, f"validate_{self.table_name}_table", None)
        if validator:
            validator(df)

    @staticmethod
    def clean_products_table(df: pd.DataFrame) -> pd.DataFrame:
        """Clean the products DataFrame."""

        # Validate category
        valid_categories = ["Electronics", "Clothing", "Food", "Furniture", "Books"]
        df["category"] = df["category"].apply(
            lambda x: x if x in valid_categories else None
        )

        # Validate brand
        valid_brands = ["BrandA", "BrandB", "BrandC", "BrandD"]
        df["brand"] = df["brand"].apply(lambda x: x if x in valid_brands else None)

        return df

    @staticmethod
    def clean_returns_table(df: pd.DataFrame) -> pd.DataFrame:
        """Clean the returns DataFrame."""

        # Validate return_reason
        valid_return_reasons = [
            "Defective",
            "Wrong Item",
            "Not Satisfied",
            "Other",
        ]
        df["return_reason"] = df["return_reason"].apply(
            lambda x: x if x in valid_return_reasons else None
        )

        return df

    @staticmethod
    def clean_orders_table(df: pd.DataFrame) -> pd.DataFrame:
        """Clean the orders DataFrame."""
        # Validate payment_method
        valid_payment_methods = ["Credit Card", "PayPal", "Bank Transfer", "Crypto"]
        df["payment_method"] = df["payment_method"].apply(
            lambda x: x if x in valid_payment_methods else None
        )
        # Validate order_status
        valid_order_status = ["Completed", "Pending", "Cancelled"]
        df["order_status"] = df["order_status"].apply(
            lambda x: x if x in valid_order_status else None
        )

        return df

    @staticmethod
    def clean_customers_table(df: pd.DataFrame) -> pd.DataFrame:
        def validate_income_range(value):
            if value.lower() not in ["low", "medium", "high"]:
                return None
            return value

        df["income_range"] = df["income_range"].apply(validate_income_range)

        def validate_age(value):
            if not isinstance(value, (int, float)) or value < 0:
                return None
            return int(value)

        df["age"] = df["age"].apply(validate_age)

        def validate_gender(value):
            if value.lower() not in ["male", "female", "non-binary"]:
                return None
            return value

        df["gender"] = df["gender"].apply(validate_gender)

        def validate_region(value):
            if value.lower() not in ["north", "south", "east", "west"]:
                return None
            return value

        df["region"] = df["region"].apply(validate_region)

        return df
