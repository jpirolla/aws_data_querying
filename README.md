# Interact with AWS using Python

This repository contains Python scripts to interact with AWS services such as S3, Athena, Glue, and Redshift. It provides functionalities to read and write data to/from S3, run queries in Athena and Redshift Spectrum, and manage databases and tables in Glue.

## Repository Files

The repository contains the following Python files:

- **`aws_data_reader.py`**: Contains functions to check for Parquet files in S3, read Parquet files from S3, run queries in Athena and Redshift Spectrum.
- **`aws_s2_csv.py`**: Contains a script to load an example DataFrame as a CSV file into S3.
- **`aws_athena_sdk.py`**: Handles creating a Glue database, uploading data to S3 as Parquet, and enabling queries in Athena.

## Requirements

To run the scripts, you'll need to install the following dependencies:

- `boto3`: Official AWS SDK for Python.
- `awswrangler`: A library for managing data on AWS (S3, Athena, Redshift, Glue, etc.).
- `pandas`: A library for data manipulation.
- `python-dotenv`: To load environment variables from a `.env` file.

You can install the required dependencies with the following command:

```bash
pip install boto3 awswrangler pandas python-dotenv
```

## Environment Setup

The scripts rely on environment variables to access AWS services. Create a `.env` file in the root directory of the repository and define the following variables:

```
AWS_PROFILE=your_aws_profile
BUCKET_NAME=your_s3_bucket_name
S3_FOLDER_PATH=s3_folder_path
ATHENA_DATABASE=athena_database_name
ATHENA_TABLE=athena_table_name
REDSHIFT_CONNECTION_NAME=redshift_connection_name
GLUE_DATABASE=glue_database_name
OBJECT_NAME=s3_object_name
```

Be sure to replace the placeholders with your actual AWS configuration values.


## Explanation of AWS Components

### Boto3

**Boto3** is the official AWS library for Python. It allows you to interact with all AWS services in a simple and efficient way. Boto3 simplifies authentication and communication with the AWS infrastructure.

### AWS SDK

The **AWS SDK** (Software Development Kit) is a set of tools that simplifies interaction with AWS services. `boto3` is the AWS SDK for Python, and it allows you to create, configure, and manage AWS resources using Python code.

### Athena

**Amazon Athena** is an interactive query service that makes it easy to analyze data in Amazon S3 using SQL. It allows you to run SQL queries directly on data stored in S3, without needing to move the data. Athena is useful when you want to query large volumes of data stored in formats like Parquet, CSV, or JSON.

### Glue

**AWS Glue** is a fully managed ETL (extract, transform, load) service. It makes it easy to prepare and move data for analytics. Glue also has a data catalog feature, which helps you manage metadata efficiently and work with services like Athena for SQL queries.

### Redshift

**Amazon Redshift** is a scalable and high-performance data warehouse service. It allows you to store and analyze large volumes of data quickly and efficiently. **Redshift Spectrum** is a feature of Redshift that allows you to run queries directly on S3 data without loading the data into Redshift, making data analysis more flexible and scalable.
