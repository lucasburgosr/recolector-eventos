from groq import RateLimitError, APIStatusError
import pandas as pd
import time
import json
import requests
from clients import groq_client
from bs4 import BeautifulSoup

groq_client = groq_client
 
# modelos_groq = ["openai/gpt-oss-120b", "openai/gpt-oss-20b", "llama-3.1-8b-instant", "llama-3.3-70b-versatile"]

def _extraer_contenido_web(url: str) -> str | None:
    """
    Extrae el contenido textual principal de una URL de forma inteligente.

    Busca en orden jerárquico las etiquetas <main>, <article> y, como último
    recurso, el <body> para aislar el contenido relevante y descartar
    menús, barras laterales y pies de página.
    """
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for element in soup(['script', 'style', 'nav', 'footer', 'aside']):
            element.decompose()

        if soup.main:
            content_container = soup.main
        elif soup.article:
            content_container = soup.article
        elif soup.find('div', {'id': 'content'}):
            content_container = soup.find('div', {'id': 'content'})
        elif soup.find('div', {'class': 'content'}):
            content_container = soup.find('div', {'class': 'content'})
        else:
            content_container = soup.body

        if not content_container:
            return None

        cleaned_text = content_container.get_text(separator=' ', strip=True)

        max_chars = 15000
        if len(cleaned_text) > max_chars:
            print(
                f"    -> Contenido principal aún es largo ({len(cleaned_text)}). Truncando.")
            cleaned_text = cleaned_text[:max_chars] + \
                "\n... [Contenido principal truncado]"

        return cleaned_text

    except requests.exceptions.RequestException as e:
        print(f"Error de red al acceder a la URL {url}: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado al procesar el contenido de {url}: {e}")
        return None


def _guardar_progreso(index: int):
    with open("./data/progreso_revision_links.txt", 'w') as f:
        f.write(str(index))


def revisar_links():
    """
    Itera sobre los links obtenidos de ```busqueda_eventos```, descarga el contenido de la página con
    BeautifulSoup y se lo envía a un LLM para que revise si es un evento relevante o si es basura.
    Genera un archivo CSV con los links que pasaron el filtro.
    """
    try:
        df = pd.read_csv("./data/resultados_busqueda.csv",
                         sep=";", low_memory=False)
    except FileNotFoundError:
        print("No se encontró el archivo ./data/resultados_busqueda.csv. Ejecute search.py primero.")
        return

    start_index = 0
    progreso = "./data/progreso_revision_links.txt"
    try:
        with open(progreso, 'r') as f:
            contenido = f.read().strip()
            if contenido:
                start_index = int(contenido)
    except FileNotFoundError:
        print("No se encontró archivo de progreso. Empezando desde el principio.")
    except ValueError:
        print("El archivo está dañado. Empezando desde el principio.")

    lineas = []
    for index, row in df.iloc[start_index:].iterrows():

        titulo = row['title']
        link = row['link']

        contenido_web = _extraer_contenido_web(link)

        if contenido_web is None:
            continue

        print(f"Cantidad de caracteres: {len(contenido_web)}")

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
            response = groq_client.chat.completions.create(
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
                model="openai/gpt-oss-20b",
                stream=False
            )

            contenido = response.choices[0].message.content.strip()

            if contenido == "No es válido":
                continue

            try:
                data_json = json.loads(contenido)
                lineas.append(data_json)
                print("El link es de un evento válido.")
            except Exception as e:
                print("La respuesta del LLM no es un JSON válido.")
                print(f"{e}")
                continue

            time.sleep(2)

        except (RateLimitError, APIStatusError) as e:
            print("Límite de requests de Groq alcanzado. Guardando progreso y saliendo.")
            print(e)
            _guardar_progreso(index=index)
            break
        except json.JSONDecodeError:
            print(
                f"Error al parsear JSON en link: {link}. Contenido: {contenido}")
            continue

    if lineas:
        datos = pd.DataFrame(lineas)
        datos.to_csv("./data/links_eventos_revisados.csv",
                     sep=";", index=False, mode='a')
        print(
            f"----\nSe guardaron {len(lineas)} links revisados en ./data/links_eventos_revisados.csv\n----")
    else:
        print("----\nNo se procesaron nuevos links o no hubo links válidos para guardar.\n----")


revisar_links()
