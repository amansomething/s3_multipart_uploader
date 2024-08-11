import math
import boto3
from botocore.client import Config
import requests

# Can use https://min.io/docs/minio/linux/index.html to mock s3
ACCESS_KEY = "USER"
SECRET_KEY = "PASS"
BUCKET_NAME = "BUCKET"
MINIO_HOST = "IP:PORT"

FILE_NAME = "test.zip"
FILE_URL = f"http://URL:PORT/{FILE_NAME}"
MB = 1024 * 1024
CHUNK_SIZE = MB * 100  # 100 MB chunks

s3 = boto3.client(
    's3',
    endpoint_url=f'http://{MINIO_HOST}',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

multipart_upload = s3.create_multipart_upload(
    Bucket=BUCKET_NAME,
    Key=FILE_NAME
)

parts = []
part_number = 1

try:
    response = requests.get(FILE_URL, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("Content-Length", 0))
    total_parts = math.ceil(total_size / CHUNK_SIZE)
    print("Total parts:", total_parts)
    print("Uploading part 1...")

    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
        if chunk:  #  Filter out keep-alive chunks
            if part_number % 10 == 0:  # Don't clutter the console too much
                print(f"Uploading part: {part_number}/{total_parts}...")
            part = s3.upload_part(
                Bucket=BUCKET_NAME,
                Key=FILE_NAME,
                PartNumber=part_number,
                UploadId=multipart_upload["UploadId"],
                Body=chunk
            )
            parts.append({
                "PartNumber": part_number,
                "ETag": part["ETag"]
            })
            part_number += 1

    print("Completing upload...")
    s3.complete_multipart_upload(
        Bucket=BUCKET_NAME,
        Key=FILE_NAME,
        UploadId=multipart_upload["UploadId"],
        MultipartUpload={"Parts": parts}
    )
except Exception as e:
    print(e)
    s3.abort_multipart_upload(
        Bucket=BUCKET_NAME,
        Key=FILE_NAME,
        UploadId=multipart_upload["UploadId"]
    )

print("Upload completed successfully!")
