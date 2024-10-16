import io
import pandas as pd
from google.cloud import storage, bigquery
from pandas_gbq import to_gbq
from datetime import datetime
from google.api_core.exceptions import NotFound

def load_csv_from_gcs(bucket_name, csv_filename):
    """Load a CSV file from Google Cloud Storage into a pandas DataFrame."""
    try:
        # Initialize a GCS client
        storage_client = storage.Client()

        # Get the bucket that the file will be downloaded from
        bucket = storage_client.bucket(bucket_name)

        # Get the blob that holds your CSV data
        blob = bucket.blob(csv_filename)

        # Download the blob into an in-memory file-like object
        csv_bytes = io.BytesIO()
        blob.download_to_file(csv_bytes)
        csv_bytes.seek(0)  # Go to the beginning to read from the object

        # Load the CSV data into a pandas DataFrame
        df = pd.read_csv(csv_bytes)
        return df

    except Exception as e:
        raise RuntimeError(f"Error loading CSV from GCS: {e}")

def mask_columns(df, column_names):
    """Mask the specified columns in the DataFrame with asterisks."""
    if df is None and df.empty:
        print("No DataFrame found or DataFrame is empty. Skipping masking.")
        return

        # Ensure column_names is a list
    if not isinstance(column_names, list):
        column_names = [column_names]  # Convert to list if it's a single string

        for column_name in column_names:
            if column_name in df.columns:
                # Masking the specified column with asterisks
                df[column_name] = df[column_name].apply(lambda x: '****' if pd.notnull(x) else x)
            else:
                raise ValueError(f"None of the specified columns {column_names} found in the DataFrame.")

        return df


def save_to_stg_bigquery(df, table_id):
    """Append the DataFrame to a BigQuery table."""
    try:
        client = bigquery.Client()
        client.get_table(table_id)  # Try to get the table
        table_exists = True
    except NotFound:
        table_exists = False
        print(f"Table does not exist: {table_id}. It will be created.")

    if not table_exists:
        # If the table doesn't exist, create it and load all data
        to_gbq(df, table_id, location = 'EU', if_exists='replace')
        print(f'Successfully created {table_id}')
    else:
        # Use to_gbq to append the DataFrame to the specified BigQuery table
        to_gbq(df, table_id, if_exists='append')  # Append data to the table
        print(f'Successfully appended {len(df)} rows to {table_id}')


def process_and_load_data(bucket_name, csv_filename, table_id, column_names):
    """Consolidate the process of loading, masking, and saving data."""
    # Step 1: Load the CSV from GCS
    df = load_csv_from_gcs(bucket_name, csv_filename)

    # Step 2: Mask the specified columns
    df = mask_columns(df, column_names)

    # Step 3: Save the DataFrame to BigQuery
    save_to_stg_bigquery(df, table_id)

