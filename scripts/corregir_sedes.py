import pandas as pd
from fuzzywuzzy import process

# Leer los archivos
df_sedes = pd.read_csv("sedes.csv", sep=";", low_memory=False)
df_eventos = pd.read_csv("eventos_entidades_asignadas.csv", sep=";", low_memory=False)

# Lista de sedes oficiales para comparar
sedes_oficiales = df_sedes["Nombre"].dropna().unique().tolist()

# Función para corregir con fuzzy matching
def corregir_sede_fuzzy(sede):
    if pd.isna(sede):
        return sede
    mejor_match, score = process.extractOne(sede, sedes_oficiales)
    if score >= 85:  # Podés ajustar este umbral según qué tan permisivo quieras ser
        return mejor_match
    else:
        return sede  # Si no hay buen match, dejar el original

# Aplicar corrección
df_eventos["sedeRaw_corregida"] = df_eventos["sedeRaw_corregida"].apply(corregir_sede_fuzzy)

# Guardar el resultado
df_eventos.to_csv("eventos_sede_corregida_final.csv", sep=";", index=False)