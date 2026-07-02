"""
extract.py

Etapa de extracción del pipeline PROFECO.

Nota de diseño: la fuente original es la API pública de PROFECO
(datos.profeco.gob.mx/quejas/consulta.php), pero al momento de construir
este pipeline el endpoint devolvía error 500 de forma consistente.
Como fallback documentado, se usa el dataset CSV publicado en
datos.gob.mx, que corresponde a la misma fuente de datos oficial.

Esta etapa valida que el archivo fuente exista y tenga la estructura
esperada antes de pasarlo a la etapa de transformación.
"""

import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_DATA_PATH = Path(__file__).parent.parent / "data" / "raw" / "quejas_buro_comercial.csv"

EXPECTED_COLUMNS = [
    "_id",
    "expediente",
    "fecha_ingreso",
    "anio_creacion",
    "estado_procesal",
    "razon_social",
    "nombre_comercial",
    "giro",
    "sector",
    "area_responsable",
    "estado",
    "motivo_reclamacion",
]


def validate_source_file(path: Path) -> None:
    """Confirma que el archivo fuente existe antes de intentar leerlo."""
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo fuente en {path}. "
            "Verifica que el CSV esté en data/raw/."
        )
    logger.info("Archivo fuente encontrado: %s (%.2f MB)", path, path.stat().st_size / 1_000_000)


def extract(path: Path = RAW_DATA_PATH) -> pd.DataFrame:
    """
    Lee el CSV crudo y valida su estructura básica.

    Returns:
        DataFrame con los datos crudos, sin transformar.
    """
    validate_source_file(path)

    logger.info("Leyendo CSV...")
    df = pd.read_csv(path, low_memory=False)

    logger.info("Filas leídas: %s", len(df))
    logger.info("Columnas encontradas: %s", list(df.columns))

    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Faltan columnas esperadas en el CSV: {missing_cols}")

    logger.info("Validación de columnas: OK")
    return df


if __name__ == "__main__":
    df = extract()
    logger.info("Extracción completada. Muestra de datos:")
    print(df.head())
