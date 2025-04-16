import boto3
import awswrangler as wr
import os
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError

# Load environment variables from .env file
load_dotenv()

# Get environment variables
profile = os.getenv("AWS_PROFILE")
bucket_name = os.getenv("BUCKET_NAME")
s3_folder_path = os.getenv("S3_FOLDER_PATH")
athena_database = os.getenv("ATHENA_DATABASE")
athena_table = os.getenv("ATHENA_TABLE")
redshift_connection_name = os.getenv("REDSHIFT_CONNECTION_NAME")

# Initialize Boto3 session
session = boto3.Session(profile_name=profile)
s3 = session.client("s3")
athena = session.client("athena")
redshift = wr.redshift

def check_parquet_files(bucket_name, s3_folder_path):
    """
    Checks if there are any Parquet files in the specified S3 bucket folder.
    
    Args:
        bucket_name (str): The name of the S3 bucket.
        s3_folder_path (str): The path within the S3 bucket to search for files.
    
    Returns:
        list: A list of Parquet file paths (keys) if found, otherwise an empty list.
    """
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder_path)
        if 'Contents' in response:
            parquet_files = [file['Key'] for file in response['Contents'] if file['Key'].endswith('.parquet')]
            return parquet_files
        else:
            print("No files found.")
            return []
    except Exception as e:
        print(f"Error accessing S3: {e}")
        return []

def read_parquet_from_s3(parquet_file):
    """
    Reads a Parquet file from S3 and returns the data as a pandas DataFrame.
    
    Args:
        parquet_file (str): The S3 URI of the Parquet file to read.
    
    Returns:
        pd.DataFrame: A pandas DataFrame containing the data from the Parquet file.
        None: If an error occurs during reading.
    """
    try:
        df = wr.s3.read_parquet(parquet_file)
        print("Data read from S3 successfully:")
        print(df)
        return df
    except Exception as e:
        print(f"Error reading Parquet from S3: {e}")
        return None

def query_athena_data():
    """
    Executes a SQL query on an Athena table and returns the results as a pandas DataFrame.
    
    Returns:
        pd.DataFrame: A pandas DataFrame containing the query result.
        None: If an error occurs during the query.
    """
    try:
        if not athena_database:
            print("Error: Athena database name is not set.")
            return None
        
        query = f"SELECT * FROM {athena_table} LIMIT 10"
        df = wr.athena.read_sql_query(query, database=athena_database)
        print("Data read from Athena successfully:")
        print(df)
        return df
    except Exception as e:
        print(f"Error reading data from Athena: {e}")
        return None

def query_redshift_spectrum():
    """
    Executes a query on Redshift Spectrum using a predefined Redshift connection.
    
    Returns:
        pd.DataFrame: A pandas DataFrame containing the query result from Redshift Spectrum.
        None: If an error occurs during the query.
    """
    try:
        if not redshift_connection_name:
            print("Error: Redshift connection name is not set.")
            return None
        
        # Connect to Redshift using the connection name
        con = wr.redshift.connect(redshift_connection_name)
        query = f"SELECT * FROM external_schema.{athena_table} LIMIT 10"
        df = wr.redshift.read_sql_query(query, con=con)
        con.close()
        print("Data read from Redshift Spectrum successfully:")
        print(df)
        return df
    except Exception as e:
        print(f"Error reading data from Redshift Spectrum: {e}")
        return None


if __name__ == "__main__":
    # Check if there are Parquet files in the specified S3 folder
    parquet_files = check_parquet_files(bucket_name, s3_folder_path)

    if parquet_files:
        print(f"Found Parquet files: {parquet_files}")
        df_s3 = read_parquet_from_s3(f"s3://{bucket_name}/{parquet_files[0]}")

    df_athena = query_athena_data()

    df_redshift = query_redshift_spectrum()
