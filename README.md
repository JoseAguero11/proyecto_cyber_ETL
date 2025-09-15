# Cybersecurity Jobs — ETL + MySQL + Power BI

Proyecto end-to-end: limpieza y modelado de salarios/roles en ciberseguridad (Kaggle) + contexto OEA.

- ETL: Python (pandas, SQLAlchemy)
- DB: MySQL 8.4 (staging + modelo dimensional)
- BI: Power BI (Import mode)

## Estructura
- data/raw: CSV original (no en repo)
- data/processed: limpio reproducible (no en repo)
- data/reference: PDF OEA y tablas auxiliares (sí)
- src: scripts Python
- docs: metodología, supuestos y validaciones

# Metodología del Proyecto

## 1. Definición del Problema
El objetivo de este proyecto es analizar el panorama salarial en el sector de la ciberseguridad, identificando los factores clave como la experiencia, la localización y los roles que influyen en la remuneración. Adicionalmente, se busca enriquecer el análisis con datos contextuales extraídos de un informe de la OEA sobre la escasez de talento en la región.

## 2. Proceso ETL (Extracción, Transformación y Carga)
* **Fuente Principal:** El dataset de salarios fue obtenido de Kaggle.
* **Limpieza:** Se utilizó un script de Python (`src/etl_salary.py`) con la librería **pandas** para realizar las siguientes transformaciones:
    * Estandarización de los nombres de las columnas.
    * Normalización de los roles de trabajo a categorías consistentes (ej. "Analyst", "Engineer").
    * Limpieza de la columna de salarios, convirtiendo el texto a un formato numérico (`DECIMAL`).
    * Creación de bandas salariales (`salary_band`) para facilitar la segmentación en Power BI.
* **Carga:** Los datos limpios fueron cargados a una base de datos **MySQL** en una tabla de staging (`cyber_salaries`) utilizando **SQLAlchemy**.

## 3. Enriquecimiento de Datos desde PDF
* Se utilizó un script (`src/extract_pdf_data.py`) con la librería **pdfplumber** para extraer texto de un informe de la OEA.
* La información clave sobre certificaciones y la escasez de talento fue identificada y estructurada en nuevos archivos CSV.

## 4. Modelado y Visualización
* Los datos de MySQL y los CSVs adicionales fueron cargados a **Power BI** en modo de importación.
* En Power Query, se realizó una fusión (Merge) para unir los datos de certificaciones con la tabla principal de salarios.
* Se crearon medidas **DAX** para calcular KPIs clave, como el conteo total de trabajos y el salario promedio.
* El dashboard final fue diseñado con un tema personalizado para presentar los insights de manera clara e interactiva.