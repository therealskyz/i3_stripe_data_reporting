from dotenv import load_dotenv
import os
import boto3
import csv

# Load environment variables from .env file
load_dotenv()

#configure boto3 to access AWS services
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')

glue = boto3.client(
        'glue',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

def get_glue_database_tables_schema(database_name):
    paginator = glue.get_paginator('get_tables')
    tables_schema = {}

    for page in paginator.paginate(DatabaseName=database_name):
        for table in page['TableList']:
            table_name = table['Name']
            columns = table['StorageDescriptor']['Columns']
            schema = [
                {
                    'Name': col['Name'],
                    'Type': col['Type'],
                    'Comment': col.get('Comment', '') 
                }
                for col in columns
            ]
            tables_schema[table_name] = schema

    return tables_schema

if __name__ == "__main__":
    database_name = 'i3-stripe-db'
    schema = get_glue_database_tables_schema(database_name)

    # Write schema to CSV
    with open('glue_schema.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Table', 'Column Name', 'Column Type', 'Comment'])  # Add Comment to header
        for table, columns in schema.items():
            for col in columns:
                writer.writerow([table, col['Name'], col['Type'], col['Comment']])

    print("Schema has been written to glue_schema.csv")