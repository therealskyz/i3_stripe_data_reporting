import duckdb
import os
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()

s3_region = 'ca-central-1'
path = 's3://i3-stripe-data/2025071115/livemode/charges/*.parquet'
REGION = 'ca-central-1'
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_region='{REGION}';")
con.execute(f"SET s3_access_key_id='{AWS_KEY}';")
con.execute(f"SET s3_secret_access_key='{AWS_SECRET}';")

query = f"SELECT invoice_id FROM '{path}'"

result = con.execute(query).fetchdf()
print(tabulate(result, headers='keys', tablefmt='psql'))
result.to_csv("output.csv", index=False)