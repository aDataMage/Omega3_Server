from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime
from uuid import UUID, uuid4
from enumsC import (
    RegionEnum,
    MaritalStatusEnum,
    EducationLevelEnum,
    EmploymentStatusEnum,
    IncomeRangeEnum,
)
import random
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from faker import Faker
from schemas.CustomerSchema import Customer
from utils.common import DATA_DIR

# Initialize Faker with additional locales
Faker.seed(0)
fake = Faker(
    [
        "it_IT",
        "en_US",
        "fr_FR",
        "de_DE",
        "es_ES",
        "pt_PT",
        "nl_NL",
        "da_DK",
    ],
)


# Function to generate and save the customer pool based on Pydantic model
def generate_and_save_customer_pool(num_customers=7000, start_date=None):
    if start_date is None:
        start_date = datetime.today().replace(day=1) - relativedelta(years=5)

    # Generating random values
    customer_ids = [str(uuid4()) for _ in range(num_customers)]
    region_values = [region.value for region in RegionEnum]
    income_range_values = [income.value for income in IncomeRangeEnum]
    marital_status_values = [status.value for status in MaritalStatusEnum]
    education_level_values = [level.value for level in EducationLevelEnum]
    employment_status_values = [status.value for status in EmploymentStatusEnum]

    # Generate the list of customer data
    customers_data = [
        {
            "customer_id": customer_id,
            "email": fake.unique.email(),
            "password_hash": fake.password(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "age": np.random.randint(18, 70),
            "gender": random.choice(["Male", "Female", "Non-Binary"]),
            "income_bracket": random.choice(income_range_values),
            "country": fake.country(),
            "region": random.choice(region_values),
            "phone_number": f"{fake.country_calling_code()}-{random.randint(1000000, 9999999)}",  # Using custom phone number provider
            "marital_status": random.choice(marital_status_values),
            "education_level": random.choice(education_level_values),
            "employment_status": random.choice(employment_status_values),
            "created_at": fake.date_between(
                start_date=start_date,
                end_date=start_date + relativedelta(years=5),
            ),
            "updated_at": datetime.now(),  # Set the same time for simplicity
        }
        for customer_id in customer_ids
    ]

    # Validate and parse the data into Pydantic models
    customers = [Customer.model_validate(customer) for customer in customers_data]

    # Convert the list of Pydantic models into a DataFrame
    customers_df = pd.DataFrame([customer.model_dump() for customer in customers])
    customers_df = customers_df[
        [
            "customer_id",
            "email",
            "password_hash",
            "first_name",
            "last_name",
            "age",
            "gender",
            "income_bracket",
            "country",
            "region",
            "phone_number",
            "marital_status",
            "education_level",
            "employment_status",
            "created_at",
            "updated_at",
        ]
    ]
    # Save the customer data to a CSV file
    customers_path = DATA_DIR / "customers"
    customers_path.mkdir(parents=True, exist_ok=True)
    customers_df.to_csv(customers_path / "customers.csv", index=False)
    print("👥 Full customer pool saved.")
    return customers_df


if __name__ == "__main__":
    # Example usage
    customer_pool = generate_and_save_customer_pool(num_customers=7000)
    print(customer_pool.head())
