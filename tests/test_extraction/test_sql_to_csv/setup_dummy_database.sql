DROP DATABASE IF EXISTS dummy_database;
CREATE DATABASE dummy_database;
\c dummy_database

CREATE TABLE payment (
    payment_id INT,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    transaction_id INT,
    counterparty_id INT,
    payment_amount NUMERIC,
    currency_id INT,
    payment_type_id INT,
    paid BOOLEAN,
    payment_date VARCHAR,
    company_ac_number INT,
    counterparty_ac_number INT
);

INSERT INTO payment 
(payment_id, created_at, last_updated, transaction_id, counterparty_id, payment_amount, currency_id, payment_type_id,
paid, payment_date, company_ac_number, counterparty_ac_number)
VALUES 
(2, TIMESTAMP '2022-11-3 14:20:52', TIMESTAMP '2022-11-3 14:20:52', 2, 15, 552548.62, 2, 3, False, '2022-11-04', 67305075, 31622269), 
(3, TIMESTAMP '2022-11-3 14:20:52', TIMESTAMP '2022-11-3 14:20:52', 3, 18, 205952.22, 3, 1, False, '2022-11-03', 81718079, 47839086), 
(5, TIMESTAMP '2022-11-3 14:20:52', TIMESTAMP '2022-11-3 14:20:52', 5, 17, 57067.20, 2, 3, False, '2022-11-06', 66213052, 91659548), 
(8, TIMESTAMP '2022-11-3 14:20:52', TIMESTAMP '2022-11-3 14:20:52', 8, 2, 254007.12, 3, 3, False, '2022-11-05', 32948439, 90135525), 
(16, TIMESTAMP '2022-11-3 14:20:52', TIMESTAMP '2022-11-3 14:20:52', 16, 15, 250459.52, 2, 1, False, '2022-11-05', 34445327, 71673373);

-- SELECT * FROM payment;

CREATE TABLE currency (
    currency_id INT,
    currency_code VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
);

INSERT INTO currency
(currency_id, currency_code, created_at, last_updated)
VALUES
(1, 'GBP', TIMESTAMP '2022-11-3 14:20:52', TIMESTAMP '2022-11-3 14:20:52');