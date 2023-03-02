from minio import Minio

def make_minio_client():
    return Minio(
        "minio:9000",
        access_key="itba-ecd",
        secret_key="seminario",
        secure=False,
    )


def create_bucket(client, bucket):
    found = client.bucket_exists(bucket)
    if found:
        print(f"Bucket '{bucket}' already exists")
        return

    print(f"Bucket '{bucket}' did not exist. Creating it...")
    client.make_bucket(bucket)
