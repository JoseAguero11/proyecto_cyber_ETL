import pdfplumber
import os

# --- 1. CONFIGURACIÓN ---
# La nueva lista de páginas que has identificado.
PAGINAS_A_EXTRAER = [12, 13, 14, 16, 17, 18, 20, 21, 23, 25, 27, 28, 29, 32, 46, 51, 52]
# Restamos 1 a cada número para obtener el índice correcto que usa pdfplumber.
indices_de_paginas = [p - 1 for p in PAGINAS_A_EXTRAER]

# Construir la ruta al archivo PDF
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
pdf_path = os.path.join(project_root, 'data', 'reference', 'oea_reporte.pdf')

print(f"Abriendo el PDF desde: {pdf_path}")

# --- 2. EXTRACCIÓN ---
try:
    with pdfplumber.open(pdf_path) as pdf:
        print("\n--- EXTRAYENDO TEXTO DE LAS PÁGINAS SELECCIONADAS ---")
        
        # Un bucle para recorrer cada índice de página que solicitaste
        for indice_de_pagina in indices_de_paginas:
            # Nos aseguramos de que el número de página exista en el PDF
            if indice_de_pagina < len(pdf.pages):
                pagina = pdf.pages[indice_de_pagina]
                texto = pagina.extract_text(layout=True, text_tolerance=2)
                
                print(f"\n--- INICIO DEL TEXTO DE LA PÁGINA {indice_de_pagina + 1} ---")
                if texto:
                    print(texto)
                else:
                    print("(Esta página no contiene texto extraíble.)")
                print(f"--- FIN DEL TEXTO DE LA PÁGINA {indice_de_pagina + 1} ---")
            else:
                print(f"\n--- OMITIENDO PÁGINA {indice_de_pagina + 1} (La página no existe) ---")

except FileNotFoundError:
    print(f"\n*** ERROR: No se encontró el archivo PDF. ***")
except Exception as e:
    print(f"\n*** Ocurrió un error inesperado: {e} ***")