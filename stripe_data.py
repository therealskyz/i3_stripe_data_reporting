import os
import boto3
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

# === Load environment variables ===
load_dotenv()

# AWS & S3
REGION = os.getenv('AWS_REGION')
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET = os.getenv('S3_BUCKET')

# PostgreSQL
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_NAME = os.getenv('PG_NAME')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
SCHEMA = 'public'

# Tables to process
TABLES = ['charges', 'products', 'prices']

# PostgreSQL engine
pg_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_NAME}"
engine = create_engine(pg_url)

# AWS S3 client
s3 = boto3.client('s3', region_name=REGION)

# DuckDB connection + S3 config
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_region='{REGION}';")
con.execute(f"SET s3_access_key_id='{AWS_KEY}';")
con.execute(f"SET s3_secret_access_key='{AWS_SECRET}';")

def get_all_date_folders(bucket):
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Delimiter='/')
    folders = []
    for page in result:
        for cp in page.get('CommonPrefixes', []):
            folder = cp['Prefix'].strip('/')
            try:
                datetime.strptime(folder, "%Y-%m-%d")
                folders.append(folder)
            except ValueError:
                continue
    return sorted(folders)

def read_parquet_from_s3(table, date_folder):
    path = f"s3://{BUCKET}/{date_folder}/livemode/{table}.parquet"
    print(f"📥 Reading {path}...")
    if table == "charges":
        return con.execute(f"SELECT id, amount, created, status FROM read_parquet('{path}')").df()
    elif table == "products":
        return con.execute(f"SELECT id, name, active FROM read_parquet('{path}')").df()
    elif table == "prices":
        return con.execute(f"SELECT id, product AS product_id, unit_amount FROM read_parquet('{path}')").df()

def load_and_deduplicate(table, df):
    staging = f"staging_{table}"
    df.to_sql(staging, engine, schema=SCHEMA, if_exists='replace', index=False, method='multi')
    with engine.begin() as conn:
        if table == 'charges':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.charges (id, amount, created, status)
                SELECT id, amount, created, status FROM {SCHEMA}.{staging}
                ON CONFLICT (id) DO UPDATE SET
                    amount = EXCLUDED.amount,
                    created = EXCLUDED.created,
                    status = EXCLUDED.status;
                TRUNCATE TABLE {SCHEMA}.{staging};
            """))
        elif table == 'products':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.products (id, name, active)
                SELECT id, name, active FROM {SCHEMA}.{staging}
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    active = EXCLUDED.active;
                TRUNCATE TABLE {SCHEMA}.{staging};
            """))
        elif table == 'prices':
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.prices (id, product_id, unit_amount)
                SELECT id, product_id, unit_amount FROM {SCHEMA}.{staging}
                ON CONFLICT (id) DO NOTHING;
                TRUNCATE TABLE {SCHEMA}.{staging};
            """))

def get_loaded_folders():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT folder_name FROM loaded_snapshots"))
        return set(row[0] for row in result)

def mark_folder_loaded(folder_name):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO loaded_snapshots (folder_name)
            VALUES (:folder)
            ON CONFLICT (folder_name) DO NOTHING;
        """), {"folder": folder_name})

def main():
    date_folders = get_all_date_folders(BUCKET)
    if not date_folders:
        print("⚠️ No date folders found in S3.")
        return

    loaded_folders = get_loaded_folders()

    if not loaded_folders:
        latest = date_folders[-1]
        print(f"🟢 Initial load from latest snapshot: {latest}")
        for table in TABLES:
            try:
                df = read_parquet_from_s3(table, latest)
                load_and_deduplicate(table, df)
            except Exception as e:
                print(f"⚠️ Skipping {table} from {latest}: {e}")
        mark_folder_loaded(latest)
    else:
        new_folders = [f for f in date_folders if f not in loaded_folders]
        if not new_folders:
            print("✅ No new data to load.")
            return

        for folder in new_folders:
            print(f"\n📁 Processing new snapshot: {folder}")
            for table in TABLES:
                try:
                    df = read_parquet_from_s3(table, folder)
                    load_and_deduplicate(table, df)
                except Exception as e:
                    print(f"⚠️ Skipping {table} from {folder}: {e}")
            mark_folder_loaded(folder)

    print("\n✅ All available Stripe data loaded into PostgreSQL.")

if __name__ == "__main__":
    main()
