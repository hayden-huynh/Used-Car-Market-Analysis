from minio import Minio, S3Error
from dotenv import load_dotenv
from io import BytesIO, StringIO
from datetime import datetime
import pandas as pd
import asyncio
import time
import json
import os


BUCKET_NAME = "used-cars"


load_dotenv()
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PWD = os.getenv("MINIO_PWD")


client = Minio(
    endpoint="localhost:9000",
    access_key=MINIO_USER,
    secret_key=MINIO_PWD,
    secure=False,
)

semaphore = asyncio.Semaphore(10)


async def upload_json(source: str, car_vin: str, car_data: dict):
    async with semaphore:
        # Parse the json file name
        file_name = f'{source}/{datetime.now().strftime("%Y-%m-%d/%H")}/{car_vin}.json'

        # Convert data dict to stream bytes
        data_bytes = BytesIO(json.dumps(car_data).encode("utf-8"))

        # Upload data to bucket
        for attempt in range(1, 4):
            try:
                client.put_object(
                    bucket_name=BUCKET_NAME,
                    object_name=file_name,
                    data=data_bytes,
                    length=len(data_bytes.getvalue()),
                    content_type="application/json",
                )
                if attempt != 1:
                    print(
                        f"^^^^^ Successfully uploaded {file_name} after {attempt} attempts ^^^^^"
                    )
                break
            except S3Error as e:
                print(f"^^^^^ Failed to upload {file_name} on attempt {attempt} ^^^^^")
                time.sleep(3)


def download_json(source: str, time_frame: str):
    prefix = f"{source}/{time_frame}"

    cars = client.list_objects(bucket_name=BUCKET_NAME, prefix=prefix, recursive=True)

    records = []
    for car in cars:
        response = client.get_object(
            bucket_name=BUCKET_NAME, object_name=car.object_name
        )
        json_bytes = response.read()
        response.close()

        try:
            json_data = json.loads(json_bytes)
            records.append(json_data)
        except Exception as e:
            print(f"Failed to parse object {car.object_name}: {e}")

    return records


def upload_csv(df: pd.DataFrame, source: str, time_frame: str):
    # Convert dataframe to CSV bytes
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = BytesIO(csv_buffer.getvalue().encode("utf-8"))

    # Write the CSV bytes to MinIO
    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{source}/{time_frame}/combined.csv",
        data=csv_bytes,
        length=len(csv_bytes.getvalue()),
        content_type="text/csv",
    )


def download_csv(source: str, time_frame: str):
    # Read the CSV file from MinIO
    response = client.get_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{source}/{time_frame}/combined.csv",
    )
    csv_bytes = response.read()
    response.close()

    # Return the comnbined CSV data as a DataFrame
    return pd.read_csv(StringIO(csv_bytes.decode("utf-8")))
