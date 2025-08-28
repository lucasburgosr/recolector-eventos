""" import pandas as pd
from dotenv import load_dotenv
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
import os
import json
import httpx
from bs4 import BeautifulSoup
import time

load_dotenv()

# --- Configuración del cliente de Google GenAI ---
try:
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    # CORRECCIÓN: Usar un modelo compatible con la API, como gemini-1.5-flash.
    # 'gemma-3-27b-it' probablemente causaba que el script fallara al iniciar.
    model = genai.GenerativeModel('gemini-1.5-flash')
except Exception as e:
    print(f"Error al configurar la API de Gemini: {e}")
    exit()

# --- Constantes de Archivos ---
INPUT_CSV_PATH = "./data/resultados_busqueda.csv"
OUTPUT_CSV_PATH = "./data/links_eventos_revisados.csv"

def get_website_text(url):
    Obtiene el contenido de texto de una URL.
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        with httpx.Client(timeout=10.0, follow_redirects=True) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        for script_or_style in soup(['script', 'style']):
            script_or_style.decompose()
        
        text = ' '.join(soup.stripped_strings)
        return text[:8000]
    except httpx.RequestError as e:
        print(f"  -> Error de red al acceder a {url}: {e}")
        return None
    except Exception as e:
        print(f"  -> Error inesperado al procesar {url}: {e}")
        return None

# --- MEJORA: Función dedicada para guardar resultados ---
def guardar_resultados_parciales(nuevas_lineas, path_salida):
    Añade nuevas líneas a un archivo CSV, creando la cabecera si es necesario.
    if not nuevas_lineas:
        print("\nNo hay nuevos resultados para guardar en esta sesión.")
        return

    df_nuevos = pd.DataFrame(nuevas_lineas)
    df_nuevos.to_csv(
        path_salida, 
        mode='a', 
        header=not os.path.exists(path_salida),
        sep=";", 
        index=False
    )
    print(f"\n✅ Se guardaron {len(nuevas_lineas)} nuevos links válidos en {path_salida}")

# --- Función Principal ---
def revisar_links():
    try:
        df_input = pd.read_csv(INPUT_CSV_PATH, sep=";")
    except FileNotFoundError:
        print(f"No se encontró el archivo {INPUT_CSV_PATH}.")
        return

    links_ya_revisados = set()
    if os.path.exists(OUTPUT_CSV_PATH):
        df_output_existente = pd.read_csv(OUTPUT_CSV_PATH, sep=";")
        links_ya_revisados = set(df_output_existente['link'])
        print(f"Se encontraron {len(links_ya_revisados)} links ya revisados. Se omitirán.")

    lineas_validas = []
    df_a_procesar = df_input[~df_input['link'].isin(links_ya_revisados)].drop_duplicates(subset=['link'])
    total_a_procesar = len(df_a_procesar)

    if total_a_procesar == 0:
        print("No hay nuevos links para procesar.")
        return

    print(f"Iniciando revisión de {total_a_procesar} nuevos links...")

    for i, row in df_a_procesar.reset_index(drop=True).iterrows():
        titulo_original = row['title']
        link = row['link']
        print(f"\nRevisando link {i+1}/{total_a_procesar}: {link}")

        contenido_web = get_website_text(link)
        if not contenido_web:
            print("  -> Sin contenido útil de la web; se omite.")
            continue

        prompt = (
            "Eres un asistente experto en clasificación de eventos en Mendoza, Argentina. "
            "Analiza el texto y responde SOLO una palabra:\n"
            " - 'VALIDO' si (1) describe un evento con fecha/rango definido y (2) el evento se realiza en la provincia de Mendoza, Argentina.\n"
            " - 'INVALIDO' en caso contrario.\n\n"
            f"Texto:\n'''{contenido_web}'''\n\n"
            "Responde UNICAMENTE con: VALIDO o INVALIDO."
        )

        try:
            response = model.generate_content(prompt)
            respuesta_llm = (response.text or "").strip().upper()
            print(f"  -> Respuesta del LLM: {respuesta_llm}")

            if respuesta_llm == 'VALIDO':
                lineas_validas.append({"titulo": titulo_original, "link": link})
            elif respuesta_llm == 'INVALIDO':
                continue
            else:
                print(f"  -> Respuesta inesperada: {respuesta_llm!r}. Se omite.")
                continue

        except google_exceptions.ResourceExhausted:
            print("⛔ Límite de requests de la API alcanzado.")
            print("Guardando progreso acumulado antes de salir...")
            guardar_resultados_parciales(lineas_validas, OUTPUT_CSV_PATH)
            return
        except Exception as e:
            # No rompas todo el bucle por errores no críticos
            print(f"  -> Error durante la llamada/procesamiento: {e}")
            continue

        time.sleep(1)


    # Guardar los resultados restantes si el bucle terminó con normalidad
    guardar_resultados_parciales(lineas_validas, OUTPUT_CSV_PATH)

revisar_links() """

from groq import Groq, RateLimitError
import pandas as pd
from dotenv import load_dotenv
import os, json, requests
from bs4 import BeautifulSoup

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def extraer_contenido_web(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for script_or_style in soup(['script', 'style']):
            script_or_style.decompose()

        text_content = soup.get_text()

        lines = (line.strip() for line_cnt, line in enumerate(
            text_content.splitlines()) if line_cnt < 1000)
        chunks = (phrase.strip() for phrase in ' '.join(lines).split("  "))
        cleaned_text = '\n'.join(chunk for chunk in chunks if chunk)

        max_chars = 15000
        if len(cleaned_text) > max_chars:
            cleaned_text = cleaned_text[:max_chars] + \
                "\n... [Contenido truncado para brevedad]"

        return cleaned_text
    except requests.exceptions.RequestException as e:
        print(f"Error al acceder a la URL {url}: {e}")
        return None
    except Exception as e:
        print(f"Error al procesar el contenido de {url}: {e}")
        return None

def revisar_links():
    """
    Itera sobre los links obtenidos de ```busqueda_eventos```, descarga el contenido de la página con
    BeautifulSoup y se lo envía a un LLM para que revise si es un evento relevante o si es basura.
    Genera un archivo CSV con los links que pasaron el filtro.
    """
    try:
        df = pd.read_csv("./data/resultados_busqueda.csv", sep=";", low_memory=False)
    except FileNotFoundError:
        print("No se encontró el archivo ./data/resultados_busqueda.csv. Ejecute search.py primero.")
        return
        
    lineas = []
    for index, row in df.iterrows():
        titulo = row['title']
        link = row['link']
        
        contenido_web = extraer_contenido_web(link)
        
        if contenido_web is None:
            continue

        prompt = (
            "Eres un asistente que revisa publicaciones en internet para encontrar "
            "eventos de reuniones. Vas a revisar contenido web extraído de diferentes sitios. "
            "Para que el evento sea considerado válido debe cumplir con las "
            "siguientes características obligatorias:\n"
            "- Debe consistir en una reunión de personas con un tema definido\n"
            "- Debe ocurrir en una fecha determinada o periodo determinado. Por "
            "ejemplo, el evento puede ser el 10/08/2025 o puede iniciar en esa "
            "fecha y extenderse hasta el 15/08/2025.\n"
            "- Debe estar situado en la provincia de Mendoza, Argentina. Cualquier "
            "otra ubicación no es válida.\n"
            "Ten en cuenta que el evento puede estar publicado por un medio o página"
            "de Mendoza pero ocurrir en otra provincia o país, en ese caso **NO** es válido. "
            "En caso de que cumpla **TODAS** las condiciones, vas a devolver el "
            f"título: {titulo} del evento y el link: {link} en un objeto JSON con las "
            "propiedades 'titulo' y 'link'. NO RESPONDAS NADA MÁS QUE LOS DATOS "
            "QUE TE SOLICITO, SOLO ESOS DOS CAMPOS. No agregues triple backtick ni "
            "la palabra 'json'. "
            "En caso de que el evento **NO SEA VÁLIDO** responde solo 'No es válido'."
        )

        try:
            print(f"Revisando link {index + 1}/{len(df)}: {link}")
            response = client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": prompt
                    },
                    {
                        "role": "user",
                        "content": f"Contenido web a revisar: {contenido_web}."
                    }
                ],
                model="openai/gpt-oss-120b",
                stream=False
            )

            contenido = response.choices[0].message.content.strip()
            
            print("RESPUESTA DE GENERAR LINK:", contenido)

            if contenido == "No es válido":
                continue

            try:
                data_json = json.loads(contenido)
                lineas.append(data_json)
            except Exception as e:
                print("La respuesta del LLM no es un JSON válido")
                print(f"{e}")
                continue

        except RateLimitError:
            print("Límite de requests de Groq alcanzado. Guardando progreso y saliendo.")
            break
        except json.JSONDecodeError:
            print(f"Error al parsear JSON en link: {link}. Contenido: {contenido}")
            continue

    if lineas:
        datos = pd.DataFrame(lineas)
        datos.to_csv("./data/links_eventos_revisados.csv", sep=";", index=False)
        print(f"Se guardaron {len(lineas)} links revisados en ./data/links_eventos_revisados.csv")
    else:
        print("No se procesaron nuevos links o no hubo links válidos para guardar.")