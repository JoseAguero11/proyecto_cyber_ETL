# -*- coding: utf-8 -*-
"""
src/etl_salary.py
Pipeline ETL:
1) Cargar CSV raw (Kaggle)
2) Explorar (reporte rápido)
3) Limpiar/Estandarizar columnas clave
4) Guardar processed CSV
5) Subir a MySQL (staging_cyber)

Ejecutar:
    python src/etl_salary.py

Requisitos:
    - .env con DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, DB_CHARSET (opcional)
    - Paquetes: pandas, sqlalchemy, python-dotenv, mysql-connector-python, openpyxl (para Excel si hiciera falta)
"""

from dotenv import load_dotenv
import os
from pathlib import Path
import sys
import re
import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# Configuración de rutas
# -------------------------
def resolve_base_dir() -> Path:
    """
    Intenta resolver la carpeta base del proyecto en función de este archivo.
    Si falla, usa la ruta que nos compartiste.
    """
    try:
        here = Path(__file__).resolve()
        base = here.parents[1]  # .../proyecto_cyber_ETL
        # Validar que existan carpetas data/src
        if (base / "data").exists() and (base / "src").exists():
            return base
    except Exception:
        pass
    # Fallback a la ruta que nos compartiste
    return Path(r"C:\Users\xpc\Documents\proyecto_cyber_ETL")

BASE_DIR = resolve_base_dir()
RAW_CSV_PATH = BASE_DIR / "data" / "raw" / "cybersecurity.csv"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_CSV_PATH = PROCESSED_DIR / "cleaned_cyber.csv"

# -------------------------
# Utilidades
# -------------------------
def quick_report(df: pd.DataFrame, n: int = 5) -> None:
    print("\n===== REPORTE RÁPIDO =====")
    print("Shape:", df.shape)
    print("\n--- HEAD ---")
    print(df.head(n))
    print("\n--- Tipos ---")
    print(df.dtypes)
    print("\n--- NA por columna (top 20) ---")
    print(df.isna().sum().sort_values(ascending=False).head(20))
    print("\n--- Describe (numéricas) ---")
    try:
        print(df.describe().T)
    except Exception as e:
        print("No se pudo describir numéricas:", e)
    print("\n==========================\n")

def load_env_and_engine():
    """Carga .env y crea el engine SQLAlchemy para MySQL."""
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_NAME = os.getenv("DB_NAME")
    DB_CHARSET = os.getenv("DB_CHARSET", "utf8mb4")

    missing = [k for k, v in {
        "DB_USER": DB_USER, "DB_PASS": DB_PASS, "DB_NAME": DB_NAME
    }.items() if not v]
    if missing:
        print(f"Faltan variables en .env: {missing}")
        sys.exit(1)

    conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset={DB_CHARSET}"
    engine = create_engine(conn_str, pool_pre_ping=True)

    # Probar conexión
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Conexión a MySQL OK")
    except Exception as e:
        print("Error conectando a MySQL:", e)
        sys.exit(1)

    return engine

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas: strip, lower, espacios -> _"""
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w_]", "", regex=True)
    )
    return df

def guess_col(df: pd.DataFrame, candidates: list) -> str | None:
    """
    Intenta adivinar una columna por candidatos (case-insensitive).
    Devuelve el nombre real en df.columns o None si no existe.
    """
    cols = list(df.columns)
    lc_map = {c.lower(): c for c in cols}
    for cand in candidates:
        lc = cand.lower()
        if lc in lc_map:
            return lc_map[lc]
        # buscar por contiene
        for c in cols:
            if lc in c.lower():
                return c
    return None

def clean_salary(series: pd.Series) -> pd.Series:
    """
    Limpia salarios en texto:
    - quita símbolos monetarios/espacios
    - deja solo dígitos y punto
    - convierte a float
    """
    s = series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")

def normalize_role(title: str | float) -> str | None:
    """
    Normaliza títulos a categorías básicas por reglas simples.
    Ajusta estas reglas según tu dataset real.
    """
    if pd.isna(title):
        return None
    t = str(title).strip().lower()

    rules = [
        (r"\b(intern|trainee)\b", "Intern"),
        (r"\b(junior|jr\.?)\b", "Junior"),
        (r"\b(senior|sr\.?)\b", "Senior"),
        (r"\b(lead|leader|principal)\b", "Lead/Principal"),
        (r"\b(manager|management|head)\b", "Manager"),
        (r"\b(architect)\b", "Architect"),
        (r"\b(analyst|analytics)\b", "Analyst"),
        (r"\b(engineer|eng)\b.*\b(security|cyber)\b", "Security Engineer"),
        (r"\b(security|cyber)\b.*\b(analyst)\b", "Security Analyst"),
        (r"\b(security|cyber)\b.*\b(consultant)\b", "Security Consultant"),
        (r"\b(gouvernance|governance|risk|compliance|grc)\b", "GRC"),
        (r"\b(soc)\b", "SOC"),
        (r"\b(pentest|penetration|red team)\b", "Pentester"),
        (r"\b(blue team|defen[cs]e)\b", "Blue Team"),
        (r"\b(cloud)\b.*\b(security)\b", "Cloud Security"),
        (r"\b(incident|ir)\b.*\b(response)\b", "Incident Response"),
        (r"\b(appsec|application security)\b", "AppSec"),
        (r"\b(data|threat)\b.*\b(analyst|intel)\b", "Threat Intel/Analyst"),
    ]

    for pattern, label in rules:
        if re.search(pattern, t):
            return label

    # fallback capitalizado
    return t.title()

def add_salary_band(s: pd.Series) -> pd.Series:
    """Crea bandas de salario útiles para filtros de BI."""
    bins = [0, 20000, 40000, 70000, 100000, 150000, 200000, 10**9]
    labels = ["<20k","20-40k","40-70k","70-100k","100-150k","150-200k",">200k"]
    return pd.cut(s, bins=bins, labels=labels, include_lowest=True)

def ensure_dirs():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# ETL principal
# -------------------------
def run_etl():
    # 1) Cargar CSV
    if not RAW_CSV_PATH.exists():
        print(f"No se encontró el CSV raw en: {RAW_CSV_PATH}")
        sys.exit(1)

    print(f"Leyendo CSV: {RAW_CSV_PATH}")
    df = pd.read_csv(RAW_CSV_PATH, encoding="utf-8", low_memory=False)

    # 2) Reporte rápido
    quick_report(df)

    # 3) Normalizar nombres de columnas
    df = standardize_columns(df)

    # 4) Detectar columnas clave (ajusta candidatos si tus nombres difieren)
    col_salary = guess_col(df, ["salary", "annual_salary", "pay", "compensation"])
    col_title  = guess_col(df, ["title", "job_title", "role", "position"])
    col_comp   = guess_col(df, ["company", "employer", "organization"])
    col_loc    = guess_col(df, ["location", "city", "country", "region"])
    col_exp    = guess_col(df, ["years_experience", "experience", "yoe", "exp"])

    # 5) Copia de trabajo
    work = df.copy()

    # 6) Limpieza de salary
    if col_salary:
        work["salary_clean"] = clean_salary(work[col_salary])
        # sanity bounds (opcional): descartar outliers absurdos
        work.loc[(work["salary_clean"] <= 0) | (work["salary_clean"] > 10_000_000), "salary_clean"] = pd.NA
        work["salary_band"] = add_salary_band(work["salary_clean"])
    else:
        work["salary_clean"] = pd.NA
        work["salary_band"] = pd.NA

    # 7) Normalizar role/title
    if col_title:
        work["role_normalized"] = work[col_title].apply(normalize_role)
    else:
        work["role_normalized"] = pd.NA

    # 8) Estandarizar company y location
    if col_comp:
        work["company_std"] = (
            work[col_comp]
            .astype(str)
            .str.strip()
            .replace({"nan": pd.NA})
        )
    else:
        work["company_std"] = pd.NA

    if col_loc:
        work["location_std"] = (
            work[col_loc]
            .astype(str)
            .str.strip()
            .str.title()
            .replace({"Nan": pd.NA, "Nan ": pd.NA, "": pd.NA})
        )
    else:
        work["location_std"] = pd.NA

    # 9) Años de experiencia
    if col_exp:
        work["years_experience_num"] = pd.to_numeric(work[col_exp], errors="coerce")
        work.loc[(work["years_experience_num"] < 0) | (work["years_experience_num"] > 60), "years_experience_num"] = pd.NA
    else:
        work["years_experience_num"] = pd.NA

    # 10) ID crudo (si existe algo parecido)
    col_id = guess_col(work, ["id", "job_id", "raw_id"])
    if col_id:
        work["raw_id"] = work[col_id].astype(str)
    else:
        # crear un id estable por índice
        work["raw_id"] = work.index.astype(str)

    # 11) Subset ordenado (para staging claro)
    ordered_cols = [
        "raw_id",
        col_title if col_title else None,
        "role_normalized",
        col_comp if col_comp else None,
        "company_std",
        col_loc if col_loc else None,
        "location_std",
        col_salary if col_salary else None,
        "salary_clean",
        "salary_band",
        col_exp if col_exp else None,
        "years_experience_num"
    ]
    ordered_cols = [c for c in ordered_cols if c is not None and c in work.columns]

    final = work[ordered_cols].copy()

    # 12) Guardar processed CSV
    ensure_dirs()
    final.to_csv(PROCESSED_CSV_PATH, index=False, encoding="utf-8")
    print(f"Procesado guardado en: {PROCESSED_CSV_PATH}")

    # 13) Subir a MySQL (staging)
    engine = load_env_and_engine()
    table_name = "staging_cyber"

    # Tip: to_sql infiere tipos; en etapa staging está bien. En DW ya usaremos SQL explícito.
    print(f"⬆️ Subiendo a MySQL → tabla `{table_name}` (if_exists='replace') ...")
    final.to_sql(table_name, con=engine, if_exists="replace", index=False, chunksize=5000)
    print("Subida completada.")

    # 14) Validación rápida en DB
    with engine.connect() as conn:
        r = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = r.scalar()
    print(f"Filas en `{table_name}`: {count}")

    print("\n ETL COMPLETADO con éxito.\n"
          "Siguiente paso: ejecutar los scripts SQL de `src/sql/` para crear dimensiones y fact.\n")


if __name__ == "__main__":
    run_etl()









