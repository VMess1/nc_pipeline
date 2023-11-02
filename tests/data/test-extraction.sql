DROP DATABASE IF EXISTS oltp_test;

CREATE DATABASE oltp_test;

\c oltp_test

CREATE TABLE department (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR,
    location VARCHAR,
    manager VARCHAR,
    create_at VARCHAR,
    last_updated VARCHAR
);

CREATE TABLE staff (
    staff_id SERIAL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    department_id INT,
    email_address VARCHAR,
    created_at VARCHAR,
    last_updated VARCHAR
);

INSERT INTO department
(   department_name,
    location,
    manager,
    create_at,
    last_updated)
VALUES
( 'departmentname-1', 'location-1', 'manager-1', '01-01-2015', '01-02-2015'),
( 'departmentname-2', 'location-2', 'manager-2', '02-01-2015', '02-02-2015'),
( 'departmentname-3', 'location-3', 'manager-3', '03-01-2015', '03-02-2015'),
( 'departmentname-4', 'location-4', 'manager-4', '04-01-2015', '04-02-2015'),
( 'departmentname-5', 'location-5', 'manager-5', '05-01-2015', '05-02-2015'),
( 'departmentname-6', 'location-6', 'manager-6', '06-01-2015', '06-02-2015'),
( 'departmentname-7', 'location-7', 'manager-7', '07-01-2015', '07-02-2015'),
( 'departmentname-8', 'location-8', 'manager-8', '08-01-2015', '08-02-2015'),
( 'departmentname-9', 'location-9', 'manager-9', '09-01-2015', '09-02-2015');

INSERT INTO staff (
    first_name,
    last_name,
    department_id,
    email_address,
    created_at,
    last_updated
)
VALUES
( 'firstname-1', 'lastname-1', 1, 'name-1@email.com', '01-01-2020', '01-02-2020'),
( 'firstname-2', 'lastname-2', 2, 'name-2@email.com', '02-01-2020', '02-02-2020'),
( 'firstname-3', 'lastname-3', 3, 'name-3@email.com', '03-01-2020', '03-02-2020'),
( 'firstname-4', 'lastname-4', 4, 'name-4@email.com', '04-01-2020', '04-02-2020'),
( 'firstname-5', 'lastname-5', 5, 'name-5@email.com', '05-01-2020', '05-02-2020'),
( 'firstname-6', 'lastname-6', 6, 'name-6@email.com', '06-01-2020', '06-02-2020'),
( 'firstname-7', 'lastname-7', 7, 'name-7@email.com', '07-01-2020', '07-02-2020'),
( 'firstname-8', 'lastname-8', 8, 'name-8@email.com', '08-01-2020', '08-02-2020'),
( 'firstname-9', 'lastname-9', 9, 'name-9@email.com', '09-01-2020', '09-02-2020');
