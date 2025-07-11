import os
import boto3
import duckdb
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timezone

# === Load environment variables ===
load_dotenv()

REGION = os.getenv('AWS_REGION')
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET = os.getenv('S3_BUCKET')

PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_NAME = os.getenv('PG_NAME')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
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
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Delimiter='/')

    folders = []
    for page in result:
        for cp in page.get('CommonPrefixes', []):
            folder = cp['Prefix'].strip('/')
            if len(folder) == 10 and folder.isdigit():
                folders.append(folder)

    return sorted(folders)[-1] if folders else None

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
            SELECT id, status, created, currency, amount
            FROM read_parquet('{path}')
        """).df()
    elif table == "invoice_line_items":
        return con.execute(f"""
            SELECT invoice_id, price_id
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
    df.to_sql(staging, engine, schema=SCHEMA, if_exists='replace', index=False, method='multi')

    with engine.begin() as conn:
        if table == 'charges':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.charges (id, status, created, currency, amount)
                SELECT id, status, created, currency, amount FROM {SCHEMA}.{staging}
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    created = EXCLUDED.created,
                    currency = EXCLUDED.currency,
                    amount = EXCLUDED.amount;
                TRUNCATE TABLE {SCHEMA}.{staging};
            """))
        elif table == 'invoice_line_items':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.invoice_line_items (invoice_id, price_id)
                SELECT invoice_id, price_id FROM {SCHEMA}.{staging}
                ON CONFLICT (invoice_id) DO UPDATE SET
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
    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

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

# === Main ===
def main():
    latest_folder = get_latest_snapshot_folder(BUCKET)
    if not latest_folder:
        print("⚠️ No snapshot folders found.")
        return

    loaded = get_loaded_folders()
    if latest_folder in loaded:
        print(f"✅ Latest snapshot '{latest_folder}' already processed.")
        return

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

if __name__ == "__main__":
    main()