import os
from minio import Minio

class MinIOClient:
    def __init__(self, bucket_name: str):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")
        self.secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

        # bucket passado como parâmetro
        self.bucket = bucket_name

        # cria o cliente
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            print(f"[OK] Bucket '{self.bucket}' criado.")
        else:
            print(f"[OK] Bucket '{self.bucket}' já existe.")

    def clean_prefix(self, prefix: str, year_month: str):
        partition_prefix = f"{prefix}/year_month={year_month}/"
        objects = self.client.list_objects(self.bucket, prefix=partition_prefix, recursive=True)
        for obj in objects:
            self.client.remove_object(self.bucket, obj.object_name)
        print(f"[CLEAN] Prefixo limpo: {partition_prefix}")

    def upload_file(self, object_name: str, file_path: str):
        self.client.fput_object(self.bucket, object_name, file_path)
        print(f"[UPLOAD] {object_name} enviado para bucket '{self.bucket}'")
