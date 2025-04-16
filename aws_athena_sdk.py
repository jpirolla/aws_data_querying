import boto3
import pandas as pd
import awswrangler as wr
import os
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError

load_dotenv()

# get environment variables
profile = os.getenv("AWS_PROFILE")
bucket = os.getenv("BUCKET_NAME")
s3_folder_path = os.getenv("S3_FOLDER_PATH")
s3_path = f"s3://{bucket}/{s3_folder_path}"
glue_database = os.getenv("GLUE_DATABASE")
athena_table = os.getenv("ATHENA_TABLE")

# initialize boto3 session and clients
session = boto3.Session(profile_name=profile)
s3 = session.client("s3")
glue = session.client("glue")

# Glue database
try:
    glue.create_database(
        DatabaseInput={
            'Name': glue_database,
            'Description': 'Database for fruit products.'
        }
    )
    print(f"Database '{glue_database}' created successfully!")

except glue.exceptions.AlreadyExistsException:
    print(f"ℹDatabase '{glue_database}' already exists!")

except Exception as e:
    print(f"Error creating Glue Database: {e}")


df = pd.DataFrame({
    "product_id": [111, 112, 113],
    "name": ["Morango", "Kiwi", "Melancia"],
    "category": ["Fruta", "Fruta", "Fruta"],
    "price": [10, 14.2, 6.8],
    "in_stock": [True, True, True]
})

# save DataFrame to S3 (Parquet), catalog it in Glue, and enable Athena queries
try:
    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=athena_table
    )
    print("Table successfully created in Athena!")

except Exception as e:
    print(f"Error creating the table: {e}")


# Read Parquet file from S3 into DataFrame
try:
    df2 = wr.s3.read_parquet(path=f"s3://{s3_path}")
    print(df2)  # Prints the content of the Parquet file from S3

except Exception as e:
    print(f"Error reading the Parquet file: {e}")