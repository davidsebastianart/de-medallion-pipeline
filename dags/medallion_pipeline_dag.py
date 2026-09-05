import os
import io
import pandas as pd
import boto3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Konfigurasi Koneksi MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Konfigurasi Target PostgreSQL Warehouse
PG_HOST = "warehouse-postgres"
PG_PORT = 5432
PG_DB = "warehouse"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def ingest_to_bronze():
    raw_file_path = "/opt/airflow/data_source/raw_data.csv"
    print(f"Membaca data dari {raw_file_path}...")
    df = pd.read_csv(raw_file_path, encoding="ISO-8859-1")
    
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)
    
    s3_client = get_s3_client()
    target_key = "raw_retail_data.parquet"
    s3_client.put_object(Bucket="bronze", Key=target_key, Body=parquet_buffer.getvalue())
    print(f"Berhasil mengunggah data Bronze ke s3://bronze/{target_key} ({len(df)} baris)")

def transform_to_silver():
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket="bronze", Key="raw_retail_data.parquet")
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    column_mapping = {
        "invoice": "invoice_no",
        "invoicedate": "invoice_date",
        "price": "unit_price",
        "customer_id": "customer_id",
        "stockcode": "stock_code",
        "description": "description",
        "quantity": "quantity",
        "country": "country"
    }
    df = df.rename(columns=column_mapping)
    
    # Cleaning
    df = df.dropna(subset=["customer_id", "description"])
    df["invoice_no"] = df["invoice_no"].astype(str)
    df = df[~df["invoice_no"].str.startswith("C")]
    
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]
    
    df["customer_id"] = df["customer_id"].astype(float).astype(int).astype(str)
    df["stock_code"] = df["stock_code"].astype(str)
    df["invoice_date"] = pd.to_datetime(df["invoice_date"])
    df["total_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    df = df.drop_duplicates()
    
    silver_buffer = io.BytesIO()
    df.to_parquet(silver_buffer, engine="pyarrow", index=False)
    silver_buffer.seek(0)
    
    s3_client.put_object(Bucket="silver", Key="cleaned_retail_data.parquet", Body=silver_buffer.getvalue())
    print(f"Berhasil mengunggah data Silver ({len(df)} baris)")

def transform_to_gold():
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket="silver", Key="cleaned_retail_data.parquet")
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    
    # 1. Dim Product (Deduped)
    dim_product = df[["stock_code", "description"]].drop_duplicates(subset=["stock_code"])
    
    # 2. Dim Customer (Deduped)
    dim_customer = df[["customer_id", "country"]].drop_duplicates(subset=["customer_id"])
    
    # 3. Fact Sales
    fct_sales = df[["invoice_no", "stock_code", "customer_id", "invoice_date", "quantity", "unit_price", "total_amount"]]
    
    # Simpan ketiganya ke Gold Bucket
    tables = {
        "dim_product.parquet": dim_product,
        "dim_customer.parquet": dim_customer,
        "fct_sales.parquet": fct_sales
    }
    
    for filename, table_df in tables.items():
        buf = io.BytesIO()
        table_df.to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)
        s3_client.put_object(Bucket="gold", Key=filename, Body=buf.getvalue())
        print(f"Uploaded s3://gold/{filename} ({len(table_df)} baris)")

def load_gold_to_postgres():
    s3_client = get_s3_client()
    
    # Download Parquet files dari Gold
    dim_prod_obj = s3_client.get_object(Bucket="gold", Key="dim_product.parquet")
    dim_cust_obj = s3_client.get_object(Bucket="gold", Key="dim_customer.parquet")
    fct_sales_obj = s3_client.get_object(Bucket="gold", Key="fct_sales.parquet")
    
    dim_product = pd.read_parquet(io.BytesIO(dim_prod_obj["Body"].read()))
    dim_customer = pd.read_parquet(io.BytesIO(dim_cust_obj["Body"].read()))
    fct_sales = pd.read_parquet(io.BytesIO(fct_sales_obj["Body"].read()))
    
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cursor = conn.cursor()
    
    # Pastikan tabel dibuat jika container postgres baru aktif
    ddl_query = """
    CREATE TABLE IF NOT EXISTS dim_product (
        stock_code VARCHAR(50) PRIMARY KEY,
        description TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id VARCHAR(50) PRIMARY KEY,
        country VARCHAR(100)
    );
    CREATE TABLE IF NOT EXISTS fct_sales (
        invoice_no VARCHAR(50),
        stock_code VARCHAR(50),
        customer_id VARCHAR(50),
        invoice_date TIMESTAMP,
        quantity INTEGER,
        unit_price NUMERIC(10, 2),
        total_amount NUMERIC(12, 2)
    );
    """
    cursor.execute(ddl_query)
    conn.commit()
    
    # Bersihkan tabel sebelum load (Idempotent Run)
    cursor.execute("TRUNCATE TABLE fct_sales, dim_product, dim_customer;")
    conn.commit()
    
    # Batch Insert: dim_product
    prod_data = [tuple(x) for x in dim_product.values]
    execute_values(cursor, "INSERT INTO dim_product (stock_code, description) VALUES %s", prod_data)
    
    # Batch Insert: dim_customer
    cust_data = [tuple(x) for x in dim_customer.values]
    execute_values(cursor, "INSERT INTO dim_customer (customer_id, country) VALUES %s", cust_data)
    
    # Batch Insert: fct_sales
    fct_sales["invoice_date"] = fct_sales["invoice_date"].astype(str)
    sales_data = [tuple(x) for x in fct_sales.values]
    execute_values(
        cursor,
        "INSERT INTO fct_sales (invoice_no, stock_code, customer_id, invoice_date, quantity, unit_price, total_amount) VALUES %s",
        sales_data,
        page_size=5000
    )
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Seluruh tabel analitik Star Schema berhasil dimuat ke PostgreSQL!")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="medallion_retail_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["medallion", "portfolio"],
) as dag:

    bronze_task = PythonOperator(
        task_id="ingest_raw_to_bronze",
        python_callable=ingest_to_bronze,
    )

    silver_task = PythonOperator(
        task_id="clean_and_transform_to_silver",
        python_callable=transform_to_silver,
    )

    gold_task = PythonOperator(
        task_id="model_star_schema_gold",
        python_callable=transform_to_gold,
    )

    load_postgres_task = PythonOperator(
        task_id="load_gold_to_postgres_warehouse",
        python_callable=load_gold_to_postgres,
    )

    # Lineage lengkap: Bronze -> Silver -> Gold -> PostgreSQL Warehouse
    bronze_task >> silver_task >> gold_task >> load_postgres_task