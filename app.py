import os
import time
import json
import boto3
import requests
from flask import Flask, jsonify

# Mini app Flask para satisfacer a Zappa (no expondremos endpoints públicos)
app = Flask(__name__)

BANREP_URL = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"
S3_BUCKET = os.getenv("S3_BUCKET")  # p.ej. dolar_raw_juank123
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "")  # opcional

s3 = boto3.client("s3")

@app.route("/")
def health():
    return jsonify({"status": "ok"})

def fetch_and_store(event=None, context=None):
    """
    Descarga el JSON crudo del BanRep y lo guarda en S3 como dolar-<epoch>.json.
    - No transforma el payload: se guarda tal cual llega.
    - <epoch> es segundos UNIX UTC al momento de la descarga.
    """
    if not S3_BUCKET:
        raise RuntimeError("Falta la variable de entorno S3_BUCKET con el nombre del bucket destino.")

    # 1) Descargar
    resp = requests.get(BANREP_URL, timeout=30)
    resp.raise_for_status()
    raw_bytes = resp.content  # tal cual llega

    # 2) Nombre de archivo
    epoch = int(time.time())
    key = f"{S3_KEY_PREFIX}dolar-{epoch}.json"

    # 3) Guardar en S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=raw_bytes,
        ContentType="application/json"
    )

    return {"bucket": S3_BUCKET, "key": key, "bytes": len(raw_bytes)}

