import enum


class RegionEnum(str, enum.Enum):
    Region1 = "Region1"
    Region2 = "Region2"
    Region3 = "Region3"
    Region4 = "Region4"
    Region5 = "Region5"
    Region6 = "Region6"
    Region7 = "Region7"
    Region8 = "Region8"
    Region9 = "Region9"
    Region10 = "Region10"


class MaritalStatusEnum(str, enum.Enum):
    single = "single"
    married = "married"
    divorced = "divorced"
    widowed = "widowed"


class EducationLevelEnum(str, enum.Enum):
    high_school = "high_school"
    associate_degree = "associate_degree"
    bachelor_degree = "bachelor_degree"
    master_degree = "master_degree"
    doctorate = "doctorate"


class EmploymentStatusEnum(str, enum.Enum):
    employed = "employed"
    unemployed = "unemployed"
    student = "student"
    retired = "retired"


class IncomeRangeEnum(str, enum.Enum):
    low = "Low"
    medium = "Medium"
    high = "High"


class BrandEnum(str, enum.Enum):
    BrandA = "BrandA"
    BrandB = "BrandB"
    BrandC = "BrandC"
    BrandD = "BrandD"
    BrandE = "BrandE"
    BrandF = "BrandF"
    BrandG = "BrandG"
    BrandH = "BrandH"
    BrandI = "BrandI"
    BrandJ = "BrandJ"


class CategoryEnum(str, enum.Enum):
    Eletronics = "electronics"
    Fashion = "fashion"
    Home_Appliances = "home_appliances"
    Beauty = "beauty"
    Sports = "sports"
    Books = "books"
    Toys = "toys"
    Automotive = "automotive"
    Groceries = "groceries"
    Furniture = "furniture"
    Health = "health"


class PaymentMethodEnum(str, enum.Enum):
    CreditCard = "credit_card"
    DebitCard = "debit_card"
    PayPal = "paypal"
    Stripe = "stripe"
    BankTransfer = "bank_transfer"
    CashOnDelivery = "cash_on_delivery"


class OrderStatusEnum(str, enum.Enum):
    Pending = "pending"
    Processing = "processing"
    Shipped = "shipped"
    Delivered = "delivered"
    Cancelled = "cancelled"
    Refunded = "refunded"


class PaymentStatusEnum(str, enum.Enum):
    Pending = "pending"
    Completed = "completed"
    Failed = "failed"
    Refunded = "refunded"


class ReturnStatusEnum(str, enum.Enum):
    Initiated = "initiated"
    Approved = "approved"
    Rejected = "rejected"
    Completed = "completed"
