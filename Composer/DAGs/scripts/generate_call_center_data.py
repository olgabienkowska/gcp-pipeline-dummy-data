from faker import Faker
import random
from io import StringIO
import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage

# Initialize Faker
fake = Faker()

def generate_call_center_data(num_records):
    """Generates fake call center data."""
    data = []
    start_date = datetime(2024, 1, 1)

    for _ in range(num_records):
        rep_id = fake.unique.random_int(min=1, max=99999)
        rep_name = fake.name()
        call_id = fake.unique.random_int(min=1, max=999999)
        client_id = fake.unique.random_int(min=1, max=99999)
        client_region = fake.city()

        # Random call duration between 1 and 60 minutes
        call_duration = random.randint(1, 60)

        # Random call start time within the last 30 days
        call_started_at = fake.date_time_between(start_date=start_date, end_date='now')

        # Call end time is start time plus duration
        call_ended_at = call_started_at + timedelta(minutes=call_duration)

        # Append the record to the data list
        data.append({
            'rep_id': rep_id,
            'rep_name': rep_name,
            'call_id': call_id,
            'client_id': client_id,
            'client_region': client_region,
            'call_duration': call_duration,
            'call_started_at': call_started_at,
            'call_ended_at': call_ended_at,
            'updated_at': datetime.now()
        })

    return data

def save_to_csv(data, filename):
    """Converts data to pandas DataFrame and saves it as a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

def file_exists_in_gcs(bucket_name, blob_name):
    """Checks if a file already exists in the Google Cloud Storage bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    return blob.exists()

def upload_csv_to_gcs(data, bucket_name, destination_blob_name):
    """Converts data to pandas DataFrame and uploads it as a CSV file to Google Cloud Storage."""
    # Convert data to DataFrame
    df = pd.DataFrame(data)

    # Convert DataFrame to CSV format in memory (no local file)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Initialize Google Cloud Storage client and upload the CSV
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload the CSV data directly to the blob
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    print(f"File uploaded to {destination_blob_name} in bucket {bucket_name}.")

def generate_and_upload_data(num_records, bucket_name):
    # Step 1: Generate call center data
    call_center_data = generate_call_center_data(num_records)

    # Step 2: Generate the filename
    filename = f'call_center_data_{datetime.today().strftime("%Y%m%d")}.csv'

    # Step 3: Check if the file already exists in GCS
    if file_exists_in_gcs(bucket_name, filename):
        print(f"File {filename} already exists in bucket {bucket_name}. Skipping upload.")
    else:
        # Step 4: Upload the data directly to GCS if the file does not exist
        upload_csv_to_gcs(call_center_data, bucket_name, filename)
