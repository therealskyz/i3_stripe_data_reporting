import duckdb
from dotenv import load_dotenv
import os
from tabulate import tabulate

# Load environment variables from .env file
load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket = os.getenv('S3_BUCKET')
s3_region = os.getenv('S3_REGION')  

con = duckdb.connect()

# Configure S3 access for DuckDB
con.execute(f"""
    SET s3_region='{s3_region}';
    SET s3_access_key_id='{aws_access_key_id}';
    SET s3_secret_access_key='{aws_secret_access_key}';
""")

# Example: List CSV files in the S3 bucket
result = con.execute(f"""
    SELECT * FROM read_parquet('s3://{s3_bucket}/2025063000/livemode/charges/*.parquet')
    LIMIT 5
""").fetchdf()

print(tabulate(result, headers='keys', tablefmt='psql'))