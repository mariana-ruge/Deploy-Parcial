import json
import boto3
import os
import pymysql

s3 = boto3.client("s3")

# Variables de entorno para conexión a RDS
RDS_HOST = os.getenv("RDS_HOST", "database-1.c8pimdlyvc6k.us-east-1.rds.amazonaws.com")
RDS_USER = os.getenv("RDS_USER", "finanzas_user")
RDS_PASS = os.getenv("RDS_PASS", "tu_password")
RDS_DB = os.getenv("RDS_DB", "finanzas")


def get_file_from_s3(bucket: str, key: str) -> str:
    """Descarga el archivo de S3 y devuelve el contenido en string."""
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def insert_into_rds(data: list):
    """Inserta los datos en RDS (tabla dolar)."""
    conn = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASS,
        db=RDS_DB,
        connect_timeout=5
    )
    try:
        with conn.cursor() as cur:
            for record in data:
                cur.execute(
                    "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)",
                    (record["fechahora"], record["valor"])
                )
        conn.commit()
    finally:
        conn.close()


def lambda_handler(event, context):
    """
    Procesa el archivo S3 (evento trigger), parsea JSON e inserta en RDS.
    """
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        file_content = get_file_from_s3(bucket, key)
        data = json.loads(file_content)

        # Si es un objeto único lo convertimos a lista
        if isinstance(data, dict):
            data = [data]

        insert_into_rds(data)

    return {"status": "success", "inserted": len(data)}
