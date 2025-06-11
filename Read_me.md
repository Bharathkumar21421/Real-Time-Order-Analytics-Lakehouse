Real-Time Orders Lakehouse Project

This project demonstrates a real-time data pipeline using AWS S3, Snowflake, and Power BI. It simulates streaming order data, stores it in a cloud bucket, loads it into Snowflake using external stages, and builds an advanced Power BI dashboard with insightful visuals and a polished UI.

📁 Folder Structure

├── simulate_orders.py          # Python script to generate simulated orders to S3
├── real_time_orders_setup.sql  # SQL commands for Snowflake setup
├── dashboard.png              # Power BI dashboard
├── screenshots/                # Dashboard screenshots
└── README.md                   # Project documentation

🧱 Step 1: Simulate Orders to S3

The simulate_orders.py script generates fake order data in CSV format and uploads it to an S3 bucket under the orders/ folder.

Libraries used: boto3, faker, uuid, pandas

Key logic:

Random order creation using faker

Uploads CSV batches to S3 using boto3

CSVs named like orders_<timestamp>.csv

python simulate_orders.py

Output files go to: s3://your-bucket-name/orders/

❄️ Step 2: Snowflake Setup

Run the SQL file real_time_orders_setup.sql to:

Create the ORDERS table

Define a storage integration for secure S3 access

Create an external stage (orders_csv_stage) that maps to the S3 folder

Load data from all CSVs into the ORDERS table

You can run the SQL using Snowflake Web UI (Snowsight).

COPY INTO PUBLIC.ORDERS
FROM @orders_csv_stage
ON_ERROR = 'CONTINUE';

Integration uses SUMMARIZE, SUMX, and TOPN DAX in Power BI for insights.

📊 Step 3: Power BI Dashboard

Open dashboard.pbix in Power BI Desktop. The dashboard includes:

✅ Features:

KPI Cards: Total Orders, Total Revenue, Avg Order Value, Unique Customers

Bar Chart: Orders by Product

Donut Chart: Revenue by Product

Column Chart: Quantity Distribution

Map: Revenue by Country

Table: Top Customers by Revenue

Slicers: Product, Country, Date

🎨 Styling:

Dark UI with consistent font and color palette

Rounded card visuals with emojis/icons

Responsive layout for readability

✅ Optional Enhancements

Schedule COPY INTO using Snowflake Tasks for automation

Publish Power BI dashboard to Power BI Service for auto-refresh

Add dynamic refresh date to footer

Built by: Bharath 
Powered by: Python · AWS S3 · Snowflake · Power BI

