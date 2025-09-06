# processor.py
import os
import json
import boto3
import pymysql
from datetime import datetime, timezone

# Conexión S3
s3 = boto3.client("s3")

# Env vars de DB
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]
DB_TABLE = os.environ.get("DB_TABLE", "dolar")  # o `dólar`, si decides

def _get_db_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.Cursor,
    )

def _parse_banrep_payload(raw_bytes: bytes):
    """
    El payload típico del servicio viene como una lista de pares:
    [
      ["1756818048000", "4040"],
      ["1756818069000", "4041.3333"],
      ...
    ]
    Retorna lista de tuplas (fechahora_utc: datetime, valor: Decimal-like string)
    """
    data = json.loads(raw_bytes.decode("utf-8"))
    rows = []
    for item in data:
        # Robustez: aceptar ["ms","valor"] o {"0":"ms","1":"valor"}
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            ms_str, val_str = item[0], item[1]
        elif isinstance(item, dict) and "0" in item and "1" in item:
            ms_str, val_str = item["0"], item["1"]
        else:
            # formatos inesperados se ignoran
            continue

        # ms a datetime UTC
        ms_int = int(str(ms_str))
        dt_utc = datetime.fromtimestamp(ms_int / 1000.0, tz=timezone.utc) \
                         .replace(tzinfo=None)  # guardaremos naive UTC en MySQL (DATETIME)

        # valor como string (MySQL lo castea a DECIMAL)
        rows.append((dt_utc, str(val_str)))
    return rows

def process_s3_event(event, context):
    """
    Handler S3 -> procesa el archivo JSON subido a dolar_raw_xxxx y lo inserta en RDS.
    """
    # Soporta múltiples records en el mismo evento
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]

        # 1) Descargar objeto
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_bytes = obj["Body"].read()

        # 2) Parsear
        rows = _parse_banrep_payload(raw_bytes)
        if not rows:
            # Log y seguir con el siguiente record
            print(f"[WARN] No se pudieron parsear filas desde {bucket}/{key}")
            continue

        # 3) Insertar en DB
        conn = _get_db_conn()
        try:
            with conn.cursor() as cur:
                # Nota: Si usas el nombre con acento, entre backticks: `dólar`
                sql = f"INSERT INTO `{DB_TABLE}` (fechahora, valor, origen_key) VALUES (%s, %s, %s)"
                values = [(dt, val, key) for (dt, val) in rows]
                cur.executemany(sql, values)
            print(f"[OK] Insertadas {len(rows)} filas desde {bucket}/{key}")
        finally:
            conn.close()

    return {"status": "ok"}
