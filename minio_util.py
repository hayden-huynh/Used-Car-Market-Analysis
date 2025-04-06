from minio import Minio
from dotenv import load_dotenv
from io import BytesIO
from datetime import datetime
import json
import os

load_dotenv()
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PWD = os.getenv("MINIO_PWD")


client = Minio(
    endpoint="localhost:9000",
    access_key=MINIO_USER,
    secret_key=MINIO_PWD,
    secure=False,
)


async def upload_json(source: str, car_data: dict):
    # Parse the json file name
    car_vin = car_data["vin"]
    file_name = f'{source}/{datetime.now().strftime("%Y-%m-%d/%H")}/{car_vin}.json'

    # Convert data dict to stream bytes
    data_bytes = BytesIO(json.dumps(car_data).encode("utf-8"))

    # Upload data to bucket
    client.put_object(
        bucket_name="used-cars",
        object_name=file_name,
        data=data_bytes,
        length=len(data_bytes.getvalue()),
        content_type="application/json",
    )


def download_json():
    pass
    # TODO
