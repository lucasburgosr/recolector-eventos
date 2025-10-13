import pandas as pd
import httpx, os, time, json
from dotenv import load_dotenv

load_dotenv()

# ----- DEFINICIÓN DE CONSTANTES Y DICCIONARIOS -----

DATA_DIR = "./data"
SEDES_PATH = os.path.join(DATA_DIR, "sedes.csv")
PROGRESO_PATH = os.path.join(DATA_DIR, "progreso.json")
RESULTADOS_PATH = os.path.join(DATA_DIR, "resultados_busqueda.csv")

API_CREDENTIALS = [
    {
        "name": "EMETUR",
        "api_key": os.getenv("EMETUR_SEARCH_API_KEY"),
        "search_engine_id": os.getenv("EMETUR_SEARCH_ENGINE")
    },
    {
        "name": "Personal",
        "api_key": os.getenv("CUSTOM_SEARCH_API_KEY"),
        "search_engine_id": os.getenv("SEARCH_ENGINE_ID")
    }
]

# ----- FUNCIONES AUXILIARES -----

def _guardar_progreso(credencial_idx, tipo_index, chunk_index):
    """Guarda el índice de la credencial, tipo de evento y chunk de sedes."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PROGRESO_PATH, "w") as f:
        progreso = {
            "credencial_idx": credencial_idx,
            "tipo_index": tipo_index,
            "chunk_index": chunk_index
        }
        json.dump(progreso, f)

def _cargar_progreso():
    """Carga el último progreso. Si no existe, empieza desde cero."""
    if os.path.exists(PROGRESO_PATH):
        with open(PROGRESO_PATH, "r") as f:
            return json.load(f)
    # Valores por defecto para el primer arranque
    return {"credencial_idx": 0, "tipo_index": 0, "chunk_index": 0}

def _guardar_resultados(resultados):
    """Normaliza y guarda una lista de resultados en el archivo CSV."""
    if not resultados:
        print("No se encontraron nuevos resultados para guardar.")
        return

    df_result = pd.json_normalize(resultados)
    columnas_a_guardar = ['title', 'link', 'snippet']
    columnas_existentes = [col for col in columnas_a_guardar if col in df_result.columns]
    
    df_result.to_csv(
        RESULTADOS_PATH, columns=columnas_existentes, mode='a',
        header=not os.path.exists(RESULTADOS_PATH), index=False, sep=";"
    )
    print(f"{len(resultados)} resultados guardados/añadidos en {RESULTADOS_PATH}")
    
# ----- WRAPPER DE LA BÚSQUEDA CON CUSTOM SEARCH API -----

def _google_search(api_key, search_engine_id, query, **params):
    """Realiza una petición a la API de Google Custom Search."""
    base_url = "https://www.googleapis.com/customsearch/v1"
    all_params = {'key': api_key, 'cx': search_engine_id, 'q': query, **params}
    
    with httpx.Client() as client:
        response = client.get(base_url, params=all_params)
        response.raise_for_status()
        return response.json()

# ----- FUNCIÓN DE BÚSQUEDA -----

def busqueda_eventos():
    """
    Búsqueda con Google Custom Search API.
    - Rota API keys cuando aparece 429 (cuota diaria agotada).
    - Persiste y reanuda progreso: credencial_idx, tipo_index y chunk_index.
    """

    tipos_evento = [
        "Jornada", "Encuentro", "Congreso", "Conferencia", "Exposición", "Seminario",
        "Evento Deportivo Internacional", "Simposio", "Convencion", "Feria"
    ]
    
    try:
        df = pd.read_csv(SEDES_PATH, sep=";")
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de sedes en la ruta '{SEDES_PATH}'.")
        return

    sedes = df["Nombre"].dropna().tolist()

    CHUNK_SIZE = 20
    sede_chunks = [sedes[i:i + CHUNK_SIZE] for i in range(0, len(sedes), CHUNK_SIZE)]

    progreso = _cargar_progreso()
    credencial_idx = progreso.get("credencial_idx", 0)
    tipo_start = progreso.get("tipo_index", 0)
    chunk_start = progreso.get("chunk_index", 0)

    credencial_idx = max(0, min(credencial_idx, len(API_CREDENTIALS) - 1)) if API_CREDENTIALS else 0
    tipo_start = max(0, min(tipo_start, len(tipos_evento) - 1)) if tipos_evento else 0
    chunk_start = max(0, min(chunk_start, len(sede_chunks) - 1)) if sede_chunks else 0

    resultados_acumulados = []
    cambio_credencial = False

    while credencial_idx < len(API_CREDENTIALS):
        credencial_actual = API_CREDENTIALS[credencial_idx]
        print(f"\n--- Usando credenciales: '{credencial_actual['name']}' ---")

        agotada_cuota_actual = False

        for tipo_idx in range(tipo_start, len(tipos_evento)):
            tipo_evento = tipos_evento[tipo_idx]
            primer_clause = f'"{tipo_evento}"'

            start_chunk_idx = chunk_start if (cambio_credencial and tipo_idx == tipo_start) else 0

            for chunk_idx in range(start_chunk_idx, len(sede_chunks)):
                chunk = sede_chunks[chunk_idx]
                query = f'{primer_clause} ({" OR ".join([f"\"{s}\"" for s in chunk])})'
                
                print(f"----- QUERY -----")
                print(query)
                print("----------")

                try:
                    print(f"Query: Buscando '{tipo_evento}' en {len(chunk)} sedes... (chunk {chunk_idx+1}/{len(sede_chunks)})")
                    response = _google_search(
                        api_key=credencial_actual["api_key"],
                        search_engine_id=credencial_actual["search_engine_id"],
                        query=f"{query} before:2025-10-01 after:2025-08-31",
                        gl="ar", cr="countryAR", lr="lang_es",
                        excludeTerms='site:.cl site:.uy site:.mx',
                        dateRestrict="d1",
                        sort="date"
                    )

                    nuevos_resultados = response.get('items', [])
                    if nuevos_resultados:
                        resultados_acumulados.extend(nuevos_resultados)
                        print(f"Éxito. Se encontraron {len(nuevos_resultados)} resultados.")
                    else:
                        print("Éxito. No se encontraron resultados para esta query.")

                    _guardar_progreso(credencial_idx, tipo_idx, chunk_idx + 1)

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        print(f"Cuota diaria agotada para '{credencial_actual['name']}'. Cambiando a la siguiente key.")

                        _guardar_progreso(credencial_idx + 1, tipo_idx, chunk_idx)

                        agotada_cuota_actual = True
                        break
                    else:
                        print(f"Error HTTP inesperado: {e}")
                        _guardar_resultados(resultados_acumulados)
                        return

                time.sleep(2.0)

            if agotada_cuota_actual:
                break

            _guardar_progreso(credencial_idx, tipo_idx + 1, 0)

        if agotada_cuota_actual:
            credencial_idx += 1
            if credencial_idx >= len(API_CREDENTIALS):
                print("Todas las credenciales han agotado su cuota.")
                break

            prog = _cargar_progreso()
            tipo_start = prog.get("tipo_index", tipo_start)
            chunk_start = prog.get("chunk_index", chunk_start)
            cambio_credencial = True
            continue

        print("\nBúsqueda completada con éxito con la credencial actual.")
        break

    print("\n--- Proceso de búsqueda finalizado ---")
    _guardar_resultados(resultados_acumulados)
       
busqueda_eventos()