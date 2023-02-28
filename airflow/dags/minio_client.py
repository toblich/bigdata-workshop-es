from minio import Minio

def make_minio_client():
    return Minio(
        "minio:9000",
        access_key="itba-ecd",
        secret_key="seminario",
        secure=False,
    )
