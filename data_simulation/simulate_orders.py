import boto3
import pandas as pd
import time
import uuid
from faker import Faker
import random
from io import StringIO
from datetime import datetime

s3 = boto3.client('s3')

BUCKET_NAME = 'your-actual-bucket-name'
FOLDER_PATH = 'orders/'

def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "product": random.choice(["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(100, 1000), 2),
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "country": faker.country()
    }

def stream_orders_to_s3(batch_size=10, interval_sec=30):
    while True:
        # Generate fake orders
        orders = [generate_order() for _ in range(batch_size)]
        df = pd.DataFrame(orders)

        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')

        # Define S3 file key
        timestamp = int(time.time())
        file_key = f"{FOLDER_PATH}orders_{timestamp}.csv"

        
        try:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=csv_buffer.getvalue())
            print(f" Uploaded {batch_size} orders to {file_key}")
        except Exception as e:
            print(f" Failed to upload to S3: {e}")

       
        time.sleep(interval_sec)

if __name__ == "__main__":
    stream_orders_to_s3(batch_size=10, interval_sec=30)
