import boto3
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError

load_dotenv()

# get environment variables
profile = os.getenv("AWS_PROFILE")
bucket_name = os.getenv("BUCKET_NAME")
object_name = os.getenv("OBJECT_NAME")

# initialize boto3 session 
session = boto3.Session(profile_name=profile)
s3 = session.client("s3")


df = pd.DataFrame({
    "product_id": [101, 102, 103],
    "name": ["Banana", "Maçã", "Laranja"],
    "category": ["Fruta", "Fruta", "Fruta"],
    "price": [3.5, 4.2, 3.8],
    "in_stock": [True, False, True]
})

# Convert to CSV in-memory
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# upload to S3
try:
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
    print(f"Upload successful to {bucket_name}/{object_name}")
except NoCredentialsError:
    print("Credentials not found.")
except Exception as e:
    print(f"An error occurred: {e}")
