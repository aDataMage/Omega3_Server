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
    low = "low"
    medium = "medium"
    high = "high"


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
    electronics = "electronics"
    fashion = "fashion"
    home_appliances = "home_appliances"
    beauty = "beauty"
    sports = "sports"
    books = "books"
    toys = "toys"
    automotive = "automotive"
    groceries = "groceries"
    furniture = "furniture"
    health = "health"


class PaymentMethodEnum(str, enum.Enum):
    credit_card = "credit_card"
    debit_card = "debit_card"
    paypal = "paypal"
    stripe = "stripe"
    bank_transfer = "bank_transfer"
    cash_on_delivery = "cash_on_delivery"


class OrderStatusEnum(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    shipped = "shipped"
    delivered = "delivered"
    cancelled = "cancelled"
    refunded = "refunded"


class PaymentStatusEnum(str, enum.Enum):
    pending = "pending"
    completed = "completed"
    failed = "failed"
    refunded = "refunded"


class ReturnStatusEnum(str, enum.Enum):
    initiated = "initiated"
    approved = "approved"
    rejected = "rejected"
    completed = "completed"
