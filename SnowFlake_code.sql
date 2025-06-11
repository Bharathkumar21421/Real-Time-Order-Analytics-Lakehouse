-- Snowflake Setup: Table, Stage, and Data Load

-- Create the ORDERS table
CREATE OR REPLACE TABLE PUBLIC.ORDERS (
    order_id STRING,
    customer_id INT,
    product STRING,
    quantity INT,
    price FLOAT,
    timestamp TIMESTAMP_NTZ,
    country STRING
);

-- Create Storage Integration (Snowflake <-> AWS S3)
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-account-id>:role/snowflake-s3-access-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket/orders/');

-- Create External Stage linked to the S3 bucket
CREATE OR REPLACE STAGE orders_csv_stage
URL = 's3://your-bucket/orders/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load all CSV files from the stage to the ORDERS table
COPY INTO PUBLIC.ORDERS
FROM @orders_csv_stage
ON_ERROR = 'CONTINUE';

--List files in the stage to verify
LIST @orders_csv_stage;


