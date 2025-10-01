CREATE TABLE IF NOT EXISTS dim_car (
    vin TEXT PRIMARY KEY,
    full_name TEXT,
    page_url TEXT,
    make TEXT,
    model TEXT,
    year_release INTEGER,
    trim_name TEXT,
    mileage INTEGER,
    mileage_per_year REAL,
    condition TEXT,
    body_type TEXT,
    exterior_color TEXT,
    interior_color TEXT,
    engine TEXT,
    -- engine_full_name TEXT,
    -- cylinder_count INTEGER,
    -- engine_size REAL,
    -- engine_config TEXT,
    fuel_type TEXT,
    -- fuel_grade TEXT,
    -- engine_hp INTEGER,
    -- engine_torque INTEGER,
    -- horsepower_rpm INTEGER,
    -- torque_rpm INTEGER,
    drivetrain TEXT,
    transmission TEXT,
    -- transmission_type TEXT,
    -- automatic_trans_type TEXT,
    -- automatic_trans_speed INTEGER,
    mpg_city REAL,
    mpg_highway REAL,
    mpg_combined REAL,
    options TEXT[]
);

CREATE TABLE IF NOT EXISTS dim_history (
    vin TEXT PRIMARY KEY,
    FOREIGN KEY (vin) REFERENCES dim_car(vin) ON DELETE CASCADE,
    days_at_dealer INTEGER,
    days_on_cargurus INTEGER,
    accident_count INTEGER,
    owner_count INTEGER,
    has_vehicle_history_report BOOLEAN,
    has_thirdparty_vehicle_damage_report BOOLEAN,
    is_fleet_vehicle BOOLEAN
    -- base_msrp REAL,
    -- destination_charge REAL
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id BIGINT PRIMARY KEY,
    seller_type TEXT,
    seller_name TEXT,
    street_address TEXT,
    city TEXT,
    postal_code TEXT,
    phone_number TEXT,
    is_franchise_dealer BOOLEAN,
    avg_rating REAL,
    review_count INTEGER
);

CREATE TABLE IF NOT EXISTS fact_listing (
    id SERIAL PRIMARY KEY,
    listing_id BIGINT,
    created_at TIMESTAMP,
    price REAL,
    expected_price REAL,
    price_diff_percent REAL,
    deal_rating TEXT,
    save_count INTEGER,
    vin TEXT references dim_car(vin) ON DELETE CASCADE,
    seller_id BIGINT references dim_seller(seller_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS car_options (
    id SERIAL PRIMARY KEY,
    option_name TEXT UNIQUE
)