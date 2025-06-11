import os
import boto3
import pandas as pd
import snowflake.connector
from io import BytesIO

# AWS S3 Config
BUCKET_NAME = 'your-bucket'
FOLDER_PATH = 'orders/'


sf_account = 'ID'
sf_user = 'Your_USer NAme'
sf_warehouse = 'COMPUTE_WH'
sf_database = 'ORDER_DB'
sf_schema = 'PUBLIC'

# Connect to S3
s3 = boto3.client('s3')

response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FOLDER_PATH)
files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
if not files:
    print("No files found.")
else:
    latest_key = files[0]['Key']
    print(" Latest file:", latest_key)

    
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_key)
    df = pd.read_csv(BytesIO(obj['Body'].read()))


    conn = snowflake.connector.connect(
        user=sf_user,
        account=sf_account,
        authenticator='externalbrowser',
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )
    cs = conn.cursor()

    # Create table if not exists
    cs.execute("""
        CREATE TABLE IF NOT EXISTS ORDERS (
            order_id STRING,
            customer_id INT,
            product STRING,
            quantity INT,
            price FLOAT,
            timestamp TIMESTAMP_NTZ,
            country STRING
        )
    """)

    # Insert rows
    for _, row in df.iterrows():
        cs.execute("""
            INSERT INTO ORDERS (order_id, customer_id, product, quantity, price, timestamp, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['order_id'],
            int(row['customer_id']),
            row['product'],
            int(row['quantity']),
            float(row['price']),
            row['timestamp'],
            row['country']
        ))

    print("Data loaded into Snowflake.")
    cs.close()
    conn.close()
