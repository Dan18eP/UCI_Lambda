"""
Manejo de Conexiones a PostgreSQL (Capa de Servicio - Serving Layer).
Cumple: ADR-004 (Vistas Precomputadas, Cero Staging).
"""

import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-serving")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "serving_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")

pool = None


def init_db_pool():
    global pool
    if pool is None:
        pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )


@contextmanager
def get_db_connection():
    global pool
    if pool is None:
        init_db_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)
