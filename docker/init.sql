CREATE SCHEMA IF NOT EXISTS tms;

CREATE TABLE tms.locations (
    location_id SERIAL PRIMARY KEY, location_code VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL, city VARCHAR(60) NOT NULL, state VARCHAR(60) NOT NULL,
    region VARCHAR(10) NOT NULL, location_type VARCHAR(20) NOT NULL);

CREATE TABLE tms.customers (
    customer_id SERIAL PRIMARY KEY, customer_code VARCHAR(12) UNIQUE NOT NULL,
    customer_name VARCHAR(120) NOT NULL, segment VARCHAR(20) NOT NULL,
    account_manager VARCHAR(80) NOT NULL, region VARCHAR(10) NOT NULL,
    payment_terms_days INT NOT NULL, status VARCHAR(10) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now());

CREATE TABLE tms.carriers (
    carrier_id SERIAL PRIMARY KEY, carrier_code VARCHAR(10) UNIQUE NOT NULL,
    carrier_name VARCHAR(100) NOT NULL, mode VARCHAR(10) NOT NULL DEFAULT 'ROAD',
    is_active BOOLEAN NOT NULL DEFAULT TRUE, updated_at TIMESTAMP NOT NULL DEFAULT now());

CREATE TABLE tms.shipments (
    shipment_id BIGSERIAL PRIMARY KEY, shipment_ref VARCHAR(20) UNIQUE NOT NULL,
    customer_id INT NOT NULL REFERENCES tms.customers(customer_id),
    carrier_id INT NOT NULL REFERENCES tms.carriers(carrier_id),
    origin_location_id INT NOT NULL REFERENCES tms.locations(location_id),
    dest_location_id INT NOT NULL REFERENCES tms.locations(location_id),
    service_level VARCHAR(15) NOT NULL, booked_at TIMESTAMP NOT NULL,
    pickup_planned_at TIMESTAMP, promised_delivery_at TIMESTAMP NOT NULL,
    weight_kg NUMERIC(10,2), volume_cbm NUMERIC(8,2), quoted_carrier_cost NUMERIC(12,2),
    currency CHAR(3) NOT NULL DEFAULT 'INR', status VARCHAR(15) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now());
CREATE INDEX ix_shipments_updated_at ON tms.shipments(updated_at);

CREATE TABLE tms.shipment_charges (
    charge_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES tms.shipments(shipment_id),
    charge_type VARCHAR(20) NOT NULL, amount NUMERIC(12,2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'INR', updated_at TIMESTAMP NOT NULL DEFAULT now());
CREATE INDEX ix_charges_updated_at ON tms.shipment_charges(updated_at);

CREATE TABLE tms.claims (
    claim_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES tms.shipments(shipment_id),
    claim_type VARCHAR(20) NOT NULL, claim_amount NUMERIC(12,2) NOT NULL,
    status VARCHAR(10) NOT NULL, opened_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now());
