from faker.providers import DynamicProvider
import random

# Create a DynamicProvider for phone numbers
phone_number_provider = DynamicProvider(
    provider_name="phone_numbers",
    elements=[
        lambda: f"+{random.choice(['1', '44', '91', '33', '49'])}-{random.randint(1000000000, 9999999999)}"
    ],
)
