"""
transform.py

Etapa de transformación del pipeline PROFECO.

Aplica limpieza y normalización sobre los datos crudos extraídos:
- Elimina filas con datos corruptos/nulos
- Corrige tipos de dato (fechas, año como entero)
- Normaliza texto (espacios extra, mayúsculas inconsistentes)
- Guarda el resultado en formato Parquet
"""

import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "processed" / "quejas_clean.parquet"

TEXT_COLUMNS = [
    "estado_procesal",
    "razon_social",
    "nombre_comercial",
    "giro",
    "sector",
    "area_responsable",
    "estado",
    "motivo_reclamacion",
]


def drop_corrupt_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas donde faltan campos críticos (ej. fecha o expediente nulo)."""
    before = len(df)
    df = df.dropna(subset=["expediente", "fecha_ingreso", "estado_procesal"])
    dropped = before - len(df)
    if dropped:
        logger.info("Filas corruptas eliminadas: %s", dropped)
    return df


def fix_types(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige tipos de dato: fechas reales y año como entero."""
    df["fecha_ingreso"] = pd.to_datetime(df["fecha_ingreso"], errors="coerce")
    df["anio_creacion"] = df["anio_creacion"].astype("Int64")
    return df


def normalize_text(df: pd.DataFrame) -> pd.DataFrame:
    """Quita espacios extra en columnas de texto."""
    for col in TEXT_COLUMNS:
        df[col] = df[col].str.strip()
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica todas las transformaciones en orden."""
    logger.info("Filas antes de transformar: %s", len(df))

    df = drop_corrupt_rows(df)
    df = fix_types(df)
    df = normalize_text(df)

    logger.info("Filas después de transformar: %s", len(df))
    return df


def save_processed(df: pd.DataFrame, path: Path = OUTPUT_PATH) -> None:
    """Guarda el DataFrame transformado en formato Parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info("Datos procesados guardados en: %s (%.2f MB)", path, path.stat().st_size / 1_000_000)


if __name__ == "__main__":
    from extract import extract

    raw_df = extract()
    clean_df = transform(raw_df)
    save_processed(clean_df)

    logger.info("Transformación completada. Muestra de datos limpios:")
    print(clean_df.head())
    print(clean_df.dtypes)
