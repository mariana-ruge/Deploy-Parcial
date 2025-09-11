# main.py
from fastapi import FastAPI, Request
import pymysql
import os

app = FastAPI()

# Variables de entorno de la BD
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

@app.get("/health")
def health():
    """Check service health"""
    return {"status": "ok"}

@app.post("/consulta")
async def consulta(request: Request):
    """Consulta valores del dólar en un rango de fechas"""
    body = await request.json()
    start = body.get("start")
    end = body.get("end")
    limit = body.get("limit", 100)

    try:
        conn = pymysql.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, database=DB_NAME
        )
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT fechahora, valor FROM dolar "
                "WHERE fechahora BETWEEN %s AND %s "
                "ORDER BY fechahora ASC LIMIT %s",
                (start, end, limit)
            )
            rows = cursor.fetchall()

        return {"count": len(rows), "items": [{"fechahora": r[0], "valor": float(r[1])} for r in rows]}

    except Exception as e:
        return {"error": str(e)}
