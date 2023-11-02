DROP DATABASE IF EXISTS oltp_test;

CREATE DATABASE oltp_test;

\c oltp_test

CREATE TABLE department (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR,
    location VARCHAR,
    manager VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
);

CREATE TABLE staff (
    staff_id SERIAL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    department_id INT,
    email_address VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
);

INSERT INTO department
(   department_name,
    location,
    manager,
    created_at,
    last_updated)
VALUES
( 'departmentname-1', 'location-1', 'manager-1', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'departmentname-2', 'location-2', 'manager-2', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'departmentname-3', 'location-3', 'manager-3', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'departmentname-4', 'location-4', 'manager-4', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'departmentname-5', 'location-5', 'manager-5', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'departmentname-6', 'location-6', 'manager-6', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'departmentname-7', 'location-7', 'manager-7', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'departmentname-8', 'location-8', 'manager-8', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'departmentname-9', 'location-9', 'manager-9', '2023-10-10 11:30:30', '2025-10-10 11:30:30');

INSERT INTO staff (
    first_name,
    last_name,
    department_id,
    email_address,
    created_at,
    last_updated
)
VALUES
( 'firstname-1', 'lastname-1', 1, 'name-1@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'firstname-2', 'lastname-2', 2, 'name-2@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'firstname-3', 'lastname-3', 3, 'name-3@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'firstname-4', 'lastname-4', 4, 'name-4@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'firstname-5', 'lastname-5', 5, 'name-5@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'firstname-6', 'lastname-6', 6, 'name-6@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'firstname-7', 'lastname-7', 7, 'name-7@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'firstname-8', 'lastname-8', 8, 'name-8@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30'),
( 'firstname-9', 'lastname-9', 9, 'name-9@email.com', '2023-10-10 11:30:30', '2023-10-10 11:30:30');
