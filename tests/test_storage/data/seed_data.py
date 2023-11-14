def get_create_location_query():
    return """
        CREATE TABLE dim_test_location (
        location_id INT PRIMARY KEY,
        address_line_1 VARCHAR NOT NULL,
        address_line_2 VARCHAR,
        district VARCHAR,
        city VARCHAR NOT NULL,
        postal_code VARCHAR NOT NULL,
        country VARCHAR NOT NULL,
        phone VARCHAR NOT NULL
        );
    """


def get_create_sales_query():
    return """
        CREATE TABLE fact_test_sales_order (
        sales_record_id SERIAL PRIMARY KEY,
        sales_order_id INT NOT NULL,
        created_date DATE NOT NULL,
        created_time TIME NOT NULL,
        last_updated_date DATE NOT NULL,
        last_updated_time TIME NOT NULL,
        units_sold INT NOT NULL,
        unit_price NUMERIC(10,2) NOT NULL,
        agreed_delivery_location INT REFERENCES dim_test_location(location_id)
        );
    """


def get_seed_location_query():
    return """
        INSERT INTO dim_test_location
        (location_id, address_line_1, address_line_2, district,
        city, postal_code, country, phone)
        VALUES
        (1, 'street_1', 'place_1', 'district_1', 'city_1', 'A111AA',
        'country_1', '1803 637401'),
        (2, 'street_2', NULL, 'district_2', 'city_2', 'B222BB',
        'country_2', '1803 637401'),
        (3, 'street_3', NULL, 'district_3', 'city_3', 'C333CC',
        'country_3', '1803 637401');
    """


def get_seed_sales_query():
    return """
        INSERT INTO fact_test_sales_order
        (sales_order_id, created_date, created_time, last_updated_date,
        last_updated_time, units_sold,
        unit_price, agreed_delivery_location)
        VALUES
        (1, '2023-10-10', '11:30:30', '2023-10-10', '11:30:30', 10, 1.5, 1),
        (2, '2023-10-10', '11:30:30', '2023-10-10', '11:30:30', 20, 1.5, 2),
        (3, '2023-10-10', '11:30:30', '2023-10-10', '11:30:30', 30, 1.5, 3);
    """
