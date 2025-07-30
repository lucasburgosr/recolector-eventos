import pandas as pd
import httpx
import os
import time
from dotenv import load_dotenv
from urllib.parse import quote

# Cargar claves API
load_dotenv()


def google_search(api_key, search_engine_id, query, **params):
    base_url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': api_key,
        'cx': search_engine_id,
        'q': query,
        **params
    }

    response = httpx.get(base_url, params=params)
    response.raise_for_status()
    return response.json()


API_KEY = os.getenv("EMETUR_SEARCH_API_KEY")
ENGINE_ID = os.getenv("EMETUR_SEARCH_ENGINE")

anno = "2025"
ubicacion = "Mendoza"
tipos_evento = ["Jornada",
                "Encuentro",   
                "Congreso",   
                "Conferencia",   
                "Exposición",   
                "Seminario",   
                "Evento Deportivo Internacional",
                "Simposio"   
                "Convencion",
                "Feria"
                ]


def busqueda_eventos():
    # Cargar CSV y preparar sedes
    df = pd.read_csv("./csvs/sedes.csv", sep=";")
    sedes = df["Nombre"].dropna().tolist()
    sede_chunks = [sedes[i:i+10]
                for i in range(0, len(sedes), 10)]  # Grupos de 10 sedes

    resultados = []
    consulta_max = 1  # Límite deseado de consultas
    consulta_count = 0

    # Construir combinaciones de queries
    for tipo_evento in tipos_evento:
        primer_clause = f'"{tipo_evento}"'
        segundo_clause = f'"{anno}"'

        for chunk in sede_chunks:
            if consulta_count >= consulta_max:
                print(f"✅ Límite de {consulta_max} consultas alcanzado.")
                break

            sedes_quoted = [f'"{s}"' for s in chunk]
            sede_clause = f'({" OR ".join(sedes_quoted)})'
            query = f"{primer_clause} {segundo_clause} {sede_clause}"

            try:
                response = google_search(
                    api_key=API_KEY,
                    search_engine_id=ENGINE_ID,
                    query=query,
                    gl="ar",
                    cr="countryAR",
                    lr="lang_es",
                    excludeTerms='site:.cl site:.uy site:.mx',
                    dateRestrict="d167"
                )
                consulta_count += 1
                resultados.extend(response.get('items', []))
                print(
                    f"✔ Consulta {consulta_count}: {tipo_evento} + grupo de sedes ({len(chunk)})")
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    print("🚨 Límite de velocidad alcanzado. Esperando 30 segundos...")
                    time.sleep(30)
                else:
                    print(f"❌ Error HTTP: {e}")
            time.sleep(2.0)  # Espaciado entre llamadas para evitar bloqueos

        if consulta_count >= consulta_max:
            break

    # Guardar resultados
    if resultados:
        df_result = pd.json_normalize(resultados)
        df_result.to_csv('./csvs/resultados_busqueda.csv', columns=[
                        'title', 'link'], index=False, sep=";")
        print("📁 Resultados guardados en resultados_busqueda.csv")
    else:
        print("⚠ No se encontraron resultados.")
