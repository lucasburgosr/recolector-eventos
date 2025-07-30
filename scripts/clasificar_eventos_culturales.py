from config.dbconfig import session
from models.evento_reuniones import Evento
from sqlalchemy.exc import SQLAlchemyError
from google.api_core import exceptions
import datetime
from bs4 import BeautifulSoup
import requests
import time
import json
from dotenv import load_dotenv
from IPython.display import Markdown
import textwrap
import google.generativeai as genai
import pandas as pd
import os
import sys
proyecto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(proyecto_dir)
# Para manejar errores de base de datos

load_dotenv()


def to_markdown(text):
    text = text.replace('•', '  *')
    return Markdown(textwrap.indent(text, '> ', predicate=lambda _: True))


GEMINI_API_KEY = os.getenv("EMETUR_SEARCH_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)

model = genai.GenerativeModel(model_name="gemini-1.5-flash-002")


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


def extraer_datos_evento(contenido_web):
    if not contenido_web:
        return None

    prompt = (
        f"Se trata de un evento cultural.\n"
        f"Analiza el siguiente contenido de página web (texto e información aparente en imágenes o banners):\n\n"
        f"{contenido_web}\n\n"
        "Extrae y devuelve la siguiente información en formato JSON (sin envoltura Markdown):\n\n"
        "1. nombreEvento: Extrae el nombre oficial del evento exactamente como aparece en el título o encabezado principal, "
        "sin agregar numeración ni texto adicional.\n\n"
        "2. tematica: Identifica la temática principal del evento y selecciona la opción que mejor se ajuste de la siguiente lista:\n"
        "   Artes visuales, Artes escénicas, Música, Cine y audiovisuales, Literatura, Patrimonio histórico, Enoturismo, Gastronomía, "
        "Danza, Teatro, Circo, Fotografía, Artesanía, Cultura tradicional, Diseño, Humor, Talleres y formación, Multidisciplinario, "
        "Tecnología aplicada al arte, Medio ambiente y arte, Moda, Otro.\n\n"
        "3. fecha: Extrae la fecha del evento tal como aparece en la página. Si hay un rango de fechas (por ejemplo, 'del 10 al 12 de julio'), "
        "devuelve el texto tal como aparece.\n\n"
        "4. horario: Extrae el horario del evento tal como aparece en la página (por ejemplo, '21:30', 'de 18 a 22 hs').\n\n"
        "5. ubicacion: Extrae el nombre de la sede o espacio donde se realiza el evento, de forma precisa (por ejemplo, 'Teatro Quintanilla').\n\n"
        "6. localidad: Extrae el nombre de la ciudad o localidad donde se realiza el evento.\n\n"
        "7. publicoObjetivo: Extrae cualquier indicación sobre el público al que está dirigido el evento (por ejemplo, 'Mayores de edad', "
        "'Todo público', 'Niños', etc.). Si no se menciona explícitamente, devuelve 'No especificado'.\n\n"
        "8. sitioWeb: Extrae la dirección del sitio web o red social oficial vinculado al evento (por ejemplo, una página de Instagram, Facebook o sitio web propio).\n\n"
        "9. entrada: Indica si la entrada es 'Gratuita', 'Paga' o 'No especificado'.\n\n"
        "10. precio: Si la entrada es paga, extrae el precio tal como aparece (por ejemplo, '$3500', 'USD 10'). "
        "Si hay precios diferenciados para locales y extranjeros, indícalo en el mismo campo (por ejemplo, '$3000 locales / $5000 extranjeros'). "
        "Si no hay información, devuelve 'No especificado'.\n\n"
        "11. modalidad: Selecciona la opción que mejor describa la modalidad del evento: Presencial, Virtual, Híbrido.\n\n"
        "Devuélveme únicamente la información en formato JSON, sin etiquetas ni formateos adicionales. "
        "Usa propiedades con estilo camelCase para las claves del JSON."
    )

    try:
        response = model.generate_content(prompt)
        return response.text
    except exceptions.ResourceExhausted as e:
        print(f"Error de cuota de la API de Gemini: {e}")
        raise
    except Exception as e:
        print(f"Error al generar contenido con el modelo: {e}")
        return None


def mapear_tipo_evento(valor_extraido):
    lista_tipos = [
        "Artes visuales",
        "Artes escénicas",
        "Música",
        "Cine y audiovisuales",
        "Literatura",
        "Patrimonio histórico",
        "Enoturismo",
        "Gastronomía",
        "Danza",
        "Teatro",
        "Circo",
        "Fotografía",
        "Artesanía",
        "Cultura tradicional",
        "Diseño",
        "Humor",
        "Talleres y formación",
        "Multidisciplinario",
        "Tecnología aplicada al arte",
        "Medio ambiente y arte",
        "Moda",
        "Otro"
    ]
    for tipo in lista_tipos:
        if tipo.lower() in valor_extraido.lower():
            return tipo
    return "Otro tipo de evento"


def buscar_localidad_sede(nombre_sede, sedes_df):
    try:
        fila = sedes_df[sedes_df['Nombre'].str.contains(
            nombre_sede, case=False, na=False)]
        if not fila.empty:
            return fila.iloc[0]['Localidad']
        else:
            return "Desconocido"
    except Exception as e:
        print(f"Error al buscar localidad de sede '{nombre_sede}': {e}")
        return "Desconocido"


def procesar_respuesta(raw_response, url, sedes_df):
    raw_response = limpiar_raw_response(raw_response)

    try:
        datos = json.loads(raw_response)
    except json.JSONDecodeError as e:
        print(f"Error procesando JSON para {url}: {e}")
        print(f"Respuesta cruda que causó el error: {raw_response}")
        return None

    # Mapeo de nombres de campos del JSON a los nombres de columnas de Evento
    # Algunos campos de salida del LLM se usan directamente, otros necesitan renombrarse.
    # Los datos brutos del LLM se guardarán en los campos 'raw' si existen.
    procesado = {
        'nombreEvento': datos.get('nombreEvento', 'Desconocido'),
        'tematica': mapear_tipo_evento(datos.get('tematica', '')),
        'fecha': formatear_fecha(datos.get('fecha')),
        'horario': datos.get('horario', 'Desconocido'),
        'ubicacion': datos.get('ubicacion', 'Desconocida'),
        'localidad': datos.get('localidad', 'Desconocida'),
        'publicoObjetivo': datos.get('publicoObjetivo', 'Indeterminado'),
        'sitioWeb': url,
        'entrada': datos.get('entrada', 'Desconocido'),
        'precio': datos.get('precio', 'Desconocido'),
        'sedeRaw': datos.get('sedeRaw', 'Desconocido')
    }

    # El campo 'Localidad' del LLM se usa para buscar la 'localidad' final
    nombre_sede_extraido = datos.get('localidad', '') or datos.get('Localidad', '')
    procesado['localidad'] = buscar_localidad_sede(
        nombre_sede_extraido, sedes_df)

    return procesado


def formatear_fecha(fecha_str):
    if not fecha_str:
        return None  # Sin valor

    fecha_str = fecha_str.strip()
    if fecha_str.lower() in ["no proporcionada", "ns/nc", "null", ""]:
        return None

    try:
        # Formato AAAA-MM-DD
        dt = datetime.datetime.strptime(fecha_str, "%Y-%m-%d")
        return dt.date()
    except ValueError:
        pass

    try:
        # Formato dd/mm/AAAA
        if "/" in fecha_str:
            dt = datetime.datetime.strptime(fecha_str, "%d/%m/%Y")
            return dt.date()
    except ValueError:
        pass

    # Mapas de meses (incluye abreviaturas)
    meses = {
        "enero": "01", "ene": "01",
        "febrero": "02", "feb": "02",
        "marzo": "03", "mar": "03",
        "abril": "04", "abr": "04",
        "mayo": "05",
        "junio": "06", "jun": "06",
        "julio": "07", "jul": "07",
        "agosto": "08", "ago": "08",
        "septiembre": "09", "setiembre": "09", "sep": "09", "set": "09",
        "octubre": "10", "oct": "10",
        "noviembre": "11", "nov": "11",
        "diciembre": "12", "dic": "12"
    }

    # Detectar rango: "del 10 al 12 de julio de 2025"
    if "del " in fecha_str and " al " in fecha_str and " de " in fecha_str:
        try:
            partes = fecha_str.lower().split("del ")[1]
            dia_inicio = partes.split(" al ")[0].strip()
            resto = partes.split(" al ")[1]
            dia_fin = resto.split(" de ")[0].strip()
            mes = resto.split(" de ")[1].strip().split()[0]
            year = resto.split(" de ")[1].strip().split()[-1]

            mes_num = meses.get(mes, "01")
            year_val = int(year) if year.isdigit() else 2025
            day_num = int(dia_inicio)

            return datetime.date(year_val, int(mes_num), day_num)
        except Exception as e:
            print(f"Error parseando rango '{fecha_str}': {e}")
            pass

    # Formato: "10 de julio de 2025" o similar
    if " de " in fecha_str:
        try:
            partes = fecha_str.lower().split(" de ")
            if len(partes) >= 3:
                day = partes[0].strip()
                month_text = partes[1].strip()
                year = partes[2].split()[0]

                mes_num = meses.get(month_text, "01")
                year_val = int(year) if year.isdigit() else 2025
                day_num = int(day)

                return datetime.date(year_val, int(mes_num), day_num)
        except Exception as e:
            print(f"Error parseando fecha textual '{fecha_str}': {e}")
            pass

    # No se pudo convertir
    return None


def limpiar_raw_response(raw_response):
    
    if raw_response.startswith("```"):
        lines = raw_response.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return raw_response

# --- Función para insertar el DataFrame en la DB ---


# def insertar_dataframe_en_mysql(df, session):
#     print("\n--- Intentando insertar datos en la base de datos MySQL ---")
#     if df.empty:
#         print("El DataFrame está vacío. No hay datos para insertar.")
#         return

#     rows_inserted = 0
#     for index, row in df.iterrows():
#         try:
#             # Crear una instancia del modelo Evento con los datos del DataFrame
#             # Mapear las columnas del DF a los atributos del modelo Evento
#             evento = Evento(
#                 nombre=row['nombre'],
#                 tipo=row['tipo'],
#                 detalle_tipo_rotacion=row['detalle_tipo_rotacion'],
#                 tema=row['tema'],
#                 fecha_edicion=row['fecha_edicion'],
#                 fecha_inicio=row['fecha_inicio'],
#                 fecha_fin=row['fecha_fin'],
#                 anio=row['anio'],
#                 mes=row['mes'],
#                 dia_inicio=row['dia_inicio'],
#                 dia_fin=row['dia_fin'],
#                 fecha_texto=row['fecha_texto']
#             )
#             session.add(evento)
#             rows_inserted += 1

#             # Comitear cada cierto número de registros para guardar progreso
#             if rows_inserted % 50 == 0:  # Comitear cada 50 registros
#                 session.commit()
#                 print(f"--> {rows_inserted} registros insertados y comiteados.")

#         except SQLAlchemyError as e:
#             session.rollback()  # Si hay un error, revierte la transacción actual
#             print(f"Error al insertar registro {index}: {e}. Revirtiendo...")
#             print(f"Datos del registro problemático: {row.to_dict()}")
#             continue  # Continúa con el siguiente registro
#         except Exception as e:
#             print(f"Error inesperado al insertar registro {index}: {e}")
#             print(f"Datos del registro problemático: {row.to_dict()}")
#             continue

#     try:
#         session.commit()  # Comitear los registros restantes
#         print(
#             f"\n¡Inserción completada! Se insertaron un total de {rows_inserted} registros.")
#     except SQLAlchemyError as e:
#         session.rollback()
#         print(f"Error final al comitear: {e}. Revirtiendo...")

if __name__ == '__main__':
    urls_df = pd.read_csv("./csvs/prueba_busqueda.csv")
    lista_urls = urls_df["link"].to_list()

    sedes_df = pd.read_csv("./csvs/sede (2) (1).csv", sep=";")

    datos_eventos = []
    should_exit_early = False

    for url in lista_urls:
        if should_exit_early:
            print(
                "Límite de cuota alcanzado. Saltando al guardado final del CSV y la DB.")
            break

        print(f"Procesando URL: {url}")
        contenido_web = extraer_contenido_web(url)
        if contenido_web:
            try:
                raw_response = extraer_datos_evento(contenido_web)
                if raw_response:
                    print("Respuesta cruda del LLM:", raw_response)
                    datos_procesados = procesar_respuesta(
                        raw_response, url, sedes_df)
                    if datos_procesados:
                        datos_eventos.append(datos_procesados)
                    else:
                        print(f"No se pudieron procesar los datos para {url}.")
                else:
                    print(f"No se obtuvo respuesta del LLM para {url}.")
            except exceptions.ResourceExhausted:
                should_exit_early = True
                print("Se alcanzó el límite de consultas de la API de Gemini. Se generará el CSV y se insertará en la DB con los datos recolectados hasta ahora.")
                break
            except Exception as e:
                print(
                    f"Error inesperado durante el procesamiento de LLM para {url}: {e}")
        else:
            print(f"No se pudo extraer contenido de la URL {url}. Saltando...")
        time.sleep(7)

    datos_eventos_filtrados = [
        evento for evento in datos_eventos if evento is not None]

    if datos_eventos_filtrados:
        df_eventos = pd.DataFrame(datos_eventos_filtrados)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"eventos_procesados_{timestamp}.csv"
        df_eventos.to_csv(output_filename, index=False, encoding='utf-8')
        print(
            f"¡Procesamiento completado! Datos guardados en '{output_filename}'")

        # try:
        #     insertar_dataframe_en_mysql(df_eventos, session)
        # finally:
        #     session.close()  # Asegurarse de cerrar la sesión
    else:
        print("No se procesó ningún evento con éxito. El archivo CSV y la inserción en DB no fueron realizados.")
