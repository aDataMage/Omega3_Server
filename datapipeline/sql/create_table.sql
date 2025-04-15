-- Enum for marital status
CREATE TYPE marital_status AS ENUM ('single', 'married', 'divorced', 'widowed');

-- Enum for education level
CREATE TYPE education_level AS ENUM (
    'high_school',
    'associate_degree',
    'bachelor_degree',
    'master_degree',
    'doctorate'
);

-- Enum for employment status
CREATE TYPE employment_status AS ENUM ('employed', 'unemployed', 'student', 'retired');

-- Enum for region (could be global or based on your requirements)
CREATE TYPE region AS ENUM (
    'Region1',
    'Region2',
    'Region3',
    'Region4',
    'Region5',
    'Region6',
    'Region7',
    'Region8',
    'Region9',
    'Region10',
);

-- Enum for product brand
CREATE TYPE brand AS ENUM (
    'BrandA',
    'BrandB',
    'BrandC',
    'BrandD',
    'BrandE',
    'BrandF',
    'BrandG',
    'BrandH',
    'BrandI',
    'BrandJ'
);

-- Enum for product category
CREATE TYPE category AS ENUM (
    'electronics',
    'fashion',
    'home_appliances',
    'beauty',
    'sports',
    'books',
    'toys',
    'automotive',
    'groceries',
    'furniture',
    'health'
);

-- Enum for payment method
CREATE TYPE payment_method AS ENUM (
    'credit_card',
    'debit_card',
    'paypal',
    'stripe',
    'bank_transfer',
    'cash_on_delivery'
);

-- Enum for payment status
CREATE TYPE payment_status AS ENUM ('pending', 'completed', 'failed', 'refunded');

-- Enum for return status
CREATE TYPE return_status AS ENUM ('initiated', 'approved', 'rejected', 'completed');

CREATE TYPE order_status AS ENUM (
    'pending',
    'processing',
    'shipped',
    'delivered',
    'cancelled',
    'refunded'
);

CREATE TYPE income_bracket AS ENUM ('low', 'medium', 'high');

CREATE TABLE
    customers (
        customer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4 (),
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        age INTEGER,
        gender VARCHAR(10),
        income_bracket income_bracket, -- Using the created ENUM type
        country VARCHAR(100),
        region region, -- Using the created ENUM type
        phone_number VARCHAR(20),
        marital_status marital_status, -- Using the created ENUM type
        education_level education_level, -- Using the created ENUM type
        employment_status employment_status, -- Using the created ENUM type
        created_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW (),
            updated_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW ()
    );

CREATE TABLE
    stores (
        store_id UUID PRIMARY KEY DEFAULT uuid_generate_v4 (),
        manager_name VARCHAR(100) NOT NULL,
        name VARCHAR(100) NOT NULL,
        created_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW (),
            updated_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW (),
            is_active BOOLEAN DEFAULT TRUE,
            region region -- Using the created ENUM type
    );

CREATE TABLE
    products (
        product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4 (),
        name VARCHAR(255) NOT NULL,
        price NUMERIC(10, 2) NOT NULL,
        cost NUMERIC(10, 2),
        brand brand, -- Using the created ENUM type
        category category, -- Using the created ENUM type
        stock_quantity INTEGER DEFAULT 0,
        created_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW (),
            updated_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW ()
    );

CREATE TABLE
    orders (
        order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4 (),
        store_id UUID NOT NULL REFERENCES stores (store_id) ON DELETE CASCADE,
        customer_id UUID NOT NULL REFERENCES customers (customer_id),
        total_amount NUMERIC(10, 2) NOT NULL,
        status order_status, -- Using the created ENUM type
        order_date DATE,
        payment_method payment_method, -- Using the created ENUM type
        payment_status payment_status, -- Using the created ENUM type
        created_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW (),
            updated_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW ()
    );

CREATE TABLE
    order_items (
        order_item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4 (),
        order_id UUID NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
        product_id UUID REFERENCES products (product_id) ON DELETE SET NULL,
        price NUMERIC(10, 2) NOT NULL,
        discount_applied NUMERIC(2, 2),
        quantity INTEGER NOT NULL,
        total_price NUMERIC(10, 2) NOT NULL
    );

CREATE TABLE
    returns (
        return_id UUID PRIMARY KEY DEFAULT uuid_generate_v4 (),
        order_item_id UUID NOT NULL REFERENCES order_items (order_item_id) ON DELETE CASCADE,
        reason TEXT,
        return_date DATE,
        refund_amount NUMERIC(10, 2),
        return_status return_status, -- Using the created ENUM type
        created_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW ()
    );