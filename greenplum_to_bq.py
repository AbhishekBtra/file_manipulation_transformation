import os
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage, bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import RealDictCursor

# CONFIG
GCS_BUCKET = "your-bucket-name"
GCS_PATH = "greenplum_daily_exports/"
BQ_DATASET = "your_dataset"
BQ_PROJECT = "your_project_id"
BQ_LOCATION = "US"
NUM_WORKERS = 20
EXPORT_DIR = "/tmp/greenplum_exports"

# Tables list (add or load from metadata DB)
TABLES = ["schema1.table1", "schema2.table2", "schema1.table3", "..."]  # total 1000

# Date for incremental load
YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

# Greenplum connection config
GP_CONN_INFO = {
    "host": "greenplum-host",
    "port": "5432",
    "dbname": "your_db",
    "user": "your_user",
    "password": "your_password"
}

# Initialize GCS and BQ clients
storage_client = storage.Client()
bq_client = bigquery.Client(project=BQ_PROJECT)

def fetch_table_data(table_name):
    schema, table = table_name.split(".")
    conn = psycopg2.connect(**GP_CONN_INFO)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = f"""
        SELECT * FROM {schema}.{table}
        WHERE CREATED_DATE >= %s
    """
    cursor.execute(query, (YESTERDAY,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def save_parquet_locally(data, table_name):
    df = pd.DataFrame(data)
    if df.empty:
        return None
    path = os.path.join(EXPORT_DIR, f"{table_name.replace('.', '_')}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    return path

def upload_to_gcs(local_file, table_name):
    dest_path = f"{GCS_PATH}{table_name.replace('.', '_')}/{datetime.date.today()}.parquet"
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(local_file)
    return f"gs://{GCS_BUCKET}/{dest_path}"

def load_to_bigquery(gcs_uri, table_name):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name.replace('.', '_')}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config, location=BQ_LOCATION
    )
    load_job.result()  # Wait for job
    return table_id

def process_table(table_name):
    try:
        print(f"Processing {table_name}")
        data = fetch_table_data(table_name)
        local_file = save_parquet_locally(data, table_name)
        if not local_file:
            print(f"No data for {table_name} on {YESTERDAY}")
            return
        gcs_uri = upload_to_gcs(local_file, table_name)
        load_to_bigquery(gcs_uri, table_name)
        print(f"Completed {table_name}")
    except Exception as e:
        print(f"Error in {table_name}: {e}")

def run_pipeline():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(process_table, table): table for table in TABLES}
        for future in as_completed(futures):
            table = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Unhandled error for {table}: {e}")

if __name__ == "__main__":
    run_pipeline()
