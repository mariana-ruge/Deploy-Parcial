import os
import json
import boto3
import requests
import uuid
from datetime import datetime, timezone
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

# Nombre base
BASE_BUCKET_NAME = os.getenv("BUCKET_NAME", "dolar-raw")

def ensure_bucket(base_name: str) -> str:
    """Crea un bucket único si no existe, y devuelve el nombre."""
    session = boto3.session.Session()
    region = session.region_name or "us-east-1"

    # Generar un nombre único usando UUID
    bucket_name = f"{base_name}-{uuid.uuid4().hex[:8]}"

    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"✅ Bucket creado: {bucket_name}")
        return bucket_name
    except ClientError as e:
        raise RuntimeError(f"Error creando el bucket: {e}")

def fetch_dolar_data(event=None, context=None):
    """
    Descarga el valor del dólar del BanRep y lo guarda en un bucket S3 único.
    """
    url = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"

    try:
        # Crear bucket nuevo siempre
        bucket_name = ensure_bucket(BASE_BUCKET_NAME)

        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Nombre de archivo con timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"dolar-{timestamp}.json"

        # Subir a S3
        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False),
            ContentType="application/json"
        )

        return {"status": "success", "file": filename, "bucket": bucket_name}

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    result = fetch_dolar_data()
    print(result)
