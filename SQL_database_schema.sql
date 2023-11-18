-- Cast columns to correct data type in orders_table
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(21),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;

-- Cast columns to correct data type in dim_users table
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE USING join_date::date;

-- Update columns in store details table
-- the blank latitude column was removed in the earlier cleaning process
-- so nothing to merge at this stage
-- The N/A values were replaced in the earlier cleaning process (so nothing to replace here)
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);


-- updates the products table
-- to remove £ sign
-- group products into groups, according to weight
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(50);
UPDATE dim_products
SET weight_class =
    CASE
        WHEN weight_kg < 2 THEN 'Light'
        WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
        WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
        WHEN weight_kg >= 140 THEN 'Truck_Required'
        ELSE NULL
    END;

-- (still on the product table)
-- rename column removed, to still_available
-- updates the column data type
ALTER TABLE dim_products 
RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products 
RENAME COLUMN "EAN" TO EAN;

ALTER TABLE dim_products 
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
ALTER COLUMN weight_kg TYPE FLOAT,
ALTER COLUMN EAN TYPE VARCHAR(20),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
ALTER COLUMN weight_class TYPE VARCHAR(15);

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL
USING CASE
	WHEN still_available = 'Still_avaliable' THEN true
	WHEN still_available = 'Removed' THEN false
	ELSE NULL
END; 

-- updates the date table with the correct data types

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(15),
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

-- updates the card table data type
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE DATE USING expiry_date::date,
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE;


-- assign primary key to the dim tables
ALTER TABLE dim_users
ADD CONSTRAINT PK_user_uuid PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details
ADD CONSTRAINT PK_card_number PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
ADD CONSTRAINT PK_store_code PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD CONSTRAINT PK_product_code PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
ADD CONSTRAINT PK_date_uuid PRIMARY KEY (date_uuid);


-- assign foreign key to the orders table

ALTER TABLE orders_table
ADD CONSTRAINT FK_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT FK_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT FK_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

-- some issue with this one
-- (product_code)=(d4-9698287C) is not present in table "dim_products"
ALTER TABLE orders_table
ADD CONSTRAINT FK_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT FK_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);