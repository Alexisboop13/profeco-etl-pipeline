"""
load.py

Etapa de carga del pipeline PROFECO.

Transforma el DataFrame limpio en un esquema tipo estrella
(fact_quejas + dimensiones) y sube cada tabla como Parquet a S3,
organizado en carpetas para ser catalogado por AWS Glue y
consultado vía Athena.
"""

import logging
from pathlib import Path

import boto3
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROCESSED_PATH = Path(__file__).parent.parent / "data" / "processed" / "quejas_clean.parquet"
WAREHOUSE_DIR = Path(__file__).parent.parent / "data" / "warehouse"
S3_BUCKET = "profeco-pipeline-alexis-2026"
S3_WAREHOUSE_PREFIX = "warehouse"


def build_dimension(df: pd.DataFrame, columns: list[str], id_name: str) -> pd.DataFrame:
    """Crea una tabla de dimensión con llave surrogate a partir de columnas únicas."""
    dim = df[columns].drop_duplicates().reset_index(drop=True)
    dim.insert(0, id_name, dim.index + 1)
    return dim


def build_star_schema(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Construye fact_quejas + dimensiones a partir del DataFrame limpio."""

    dim_proveedor = build_dimension(df, ["razon_social", "nombre_comercial"], "proveedor_id")
    dim_ubicacion = build_dimension(df, ["estado", "area_responsable"], "ubicacion_id")
    dim_categoria = build_dimension(df, ["giro", "sector"], "categoria_id")
    dim_motivo = build_dimension(df, ["motivo_reclamacion"], "motivo_id")

    fact = df.merge(dim_proveedor, on=["razon_social", "nombre_comercial"], how="left")
    fact = fact.merge(dim_ubicacion, on=["estado", "area_responsable"], how="left")
    fact = fact.merge(dim_categoria, on=["giro", "sector"], how="left")
    fact = fact.merge(dim_motivo, on=["motivo_reclamacion"], how="left")

    fact_quejas = fact[[
        "_id", "expediente", "fecha_ingreso", "anio_creacion", "estado_procesal",
        "proveedor_id", "ubicacion_id", "categoria_id", "motivo_id",
    ]]

    logger.info("fact_quejas: %s filas", len(fact_quejas))
    logger.info("dim_proveedor: %s filas únicas", len(dim_proveedor))
    logger.info("dim_ubicacion: %s filas únicas", len(dim_ubicacion))
    logger.info("dim_categoria: %s filas únicas", len(dim_categoria))
    logger.info("dim_motivo: %s filas únicas", len(dim_motivo))

    return {
        "fact_quejas": fact_quejas,
        "dim_proveedor": dim_proveedor,
        "dim_ubicacion": dim_ubicacion,
        "dim_categoria": dim_categoria,
        "dim_motivo": dim_motivo,
    }


def save_and_upload(tables: dict[str, pd.DataFrame]) -> None:
    """Guarda cada tabla localmente y la sube a S3 en su propia carpeta (formato Hive-style)."""
    s3 = boto3.client("s3")
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    for name, table in tables.items():
        local_path = WAREHOUSE_DIR / f"{name}.parquet"
        table.to_parquet(local_path, index=False)

        s3_key = f"{S3_WAREHOUSE_PREFIX}/{name}/{name}.parquet"
        s3.upload_file(str(local_path), S3_BUCKET, s3_key)
        logger.info("Subido: s3://%s/%s", S3_BUCKET, s3_key)


if __name__ == "__main__":
    from extract import extract
    from transform import transform

    raw_df = extract()
    clean_df = transform(raw_df)
    tables = build_star_schema(clean_df)
    save_and_upload(tables)

    logger.info("Carga completada. Esquema estrella disponible en S3.")
