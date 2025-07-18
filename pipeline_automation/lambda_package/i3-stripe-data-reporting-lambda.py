import os
import json
import boto3
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# === Load environment variables ===
def get_secret(secret_name):
    region_name = "ca-central-1"
    
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

AWS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
REGION = os.environ['AWS_REGION']
BUCKET = os.environ['S3_BUCKET']

pg_secrets = get_secret('dev/i3_Stripe_Data_Pipeline/PostgreSQL')
PG_HOST = pg_secrets['host']
PG_PORT = pg_secrets['port']
PG_NAME = pg_secrets['dbname']
PG_USER = pg_secrets['username']
PG_PASSWORD = pg_secrets['password']
SCHEMA = 'finance'

TABLES = ['charges', 'invoice_line_items', 'prices', 'products']

# PostgreSQL connection
pg_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_NAME}"
engine = create_engine(pg_url)

# AWS S3 and DuckDB
s3 = boto3.client('s3', region_name=REGION)
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_region='{REGION}';")
con.execute(f"SET s3_access_key_id='{AWS_KEY}';")
con.execute(f"SET s3_secret_access_key='{AWS_SECRET}';")

# === Step 1: Find latest snapshot folder ===
def get_latest_snapshot_folder(bucket):
    folders = get_snapshot_folders_with_success_marker(bucket)
    return folders[-1] if folders else None

# === Step 2: Track loaded folders ===
def get_loaded_folders():
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT folder_name FROM {SCHEMA}.loaded_snapshots"))
        return set(row[0] for row in result)

def mark_folder_loaded(folder):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO finance.loaded_snapshots (folder_name)
            VALUES (:folder)
            ON CONFLICT (folder_name) DO NOTHING;
        """), {"folder": folder})

# === Step 3: Read from Parquet ===
def read_parquet_from_s3(table, snapshot_folder):
    path = f"s3://{BUCKET}/{snapshot_folder}/livemode/{table}/*.parquet"
    print(f"📥 Reading: {path}")

    if table == "charges":
        return con.execute(f"""
            SELECT id, status, invoice_id, created, currency, amount
            FROM read_parquet('{path}')
        """).df()
    elif table == "invoice_line_items":
        return con.execute(f"""
            SELECT id, invoice_id, price_id
            FROM read_parquet('{path}')
        """).df()
    elif table == "prices":
        return con.execute(f"""
            SELECT id, product_id
            FROM read_parquet('{path}')
        """).df()
    elif table == "products":
        return con.execute(f"""
            SELECT id, name
            FROM read_parquet('{path}')
        """).df()

# === Step 4: Insert or update in PostgreSQL ===
def load_and_deduplicate(table, df):
    staging = f"staging_{table}"
    df.to_sql(staging, engine, schema=SCHEMA, if_exists='append', index=False, method='multi')

    with engine.begin() as conn:
        if table == 'charges':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.charges (id, status, invoice_id, created, currency, amount)
                SELECT id, status, invoice_id, created, currency, amount FROM {SCHEMA}.{staging}
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    invoice_id = EXCLUDED.invoice_id,
                    created = EXCLUDED.created,
                    currency = EXCLUDED.currency,
                    amount = EXCLUDED.amount;
                TRUNCATE TABLE {SCHEMA}.{staging};
            """))
        elif table == 'invoice_line_items':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.invoice_line_items (id, invoice_id, price_id)
                SELECT id, invoice_id, price_id FROM {SCHEMA}.{staging}
                ON CONFLICT (id) DO UPDATE SET
                    invoice_id = EXCLUDED.invoice_id,
                    price_id = EXCLUDED.price_id;
                TRUNCATE TABLE {SCHEMA}.{staging};
            """))
        elif table == 'prices':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.prices (id, product_id)
                SELECT id, product_id FROM {SCHEMA}.{staging}
                ON CONFLICT (id) DO UPDATE SET
                    product_id = EXCLUDED.product_id;
                TRUNCATE TABLE {SCHEMA}.{staging};
            """))
        elif table == 'products':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.products (id, name)
                SELECT id, name FROM {SCHEMA}.{staging}
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name;
                TRUNCATE TABLE {SCHEMA}.{staging};
            """))

# === Step 5: Safe snapshot cleanup ===
def delete_previous_day_snapshots(bucket):
    today_str = datetime.now().strftime("%Y%m%d")

    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Delimiter="/")

    for page in result:
        for cp in page.get('CommonPrefixes', []):
            folder = cp['Prefix'].strip('/')
            if len(folder) == 10 and folder.isdigit():
                if not folder.startswith(today_str):
                    print(f"🗑️ Deleting old snapshot folder: {folder}")
                    delete_objects_under_prefix(bucket, folder + '/')

def delete_objects_under_prefix(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in result:
        if "Contents" in page:
            keys = [{'Key': obj['Key']} for obj in page['Contents']]
            if keys:
                print(f"   Deleting {len(keys)} objects under: {prefix}")
                s3.delete_objects(Bucket=bucket, Delete={'Objects': keys})

def get_snapshot_folders_with_success_marker(bucket):
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Delimiter='/')

    valid_folders = []
    for page in result:
        for cp in page.get('CommonPrefixes', []):
            folder = cp['Prefix'].strip('/')
            if len(folder) == 10 and folder.isdigit():
                success_key = f"{folder}/livemode/coreapi_SUCCESS"
                try:
                    s3.head_object(Bucket=bucket, Key=success_key)
                    valid_folders.append(folder)
                except s3.exceptions.ClientError:
                    # coreapi_SUCCESS does not exist; skip
                    continue
    return sorted(valid_folders)

def lambda_handler(event, context):
    latest_folder = get_latest_snapshot_folder(BUCKET)
    if not latest_folder:
        print("⚠️ No snapshot folders found.")
        return {"status": "No snapshot folders found"}

    loaded = get_loaded_folders()
    if latest_folder in loaded:
        print(f"✅ Latest snapshot '{latest_folder}' already processed.")
        return {"status": f"Latest snapshot '{latest_folder}' already processed"}

    print(f"\n📂 Processing snapshot: {latest_folder}")
    for table in TABLES:
        try:
            df = read_parquet_from_s3(table, latest_folder)
            load_and_deduplicate(table, df)
        except Exception as e:
            print(f"⚠️ Skipping {table} from {latest_folder}: {e}")

    mark_folder_loaded(latest_folder)
    delete_previous_day_snapshots(BUCKET)

    print("\n✅ Done: Snapshot loaded and old folders deleted.")
    return {"status": "Success", "latest_folder": latest_folder}