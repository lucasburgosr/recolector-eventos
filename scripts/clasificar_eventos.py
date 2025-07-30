import os
import sys
proyecto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(proyecto_dir)
import pandas as pd
import google.generativeai as genai
import textwrap
from IPython.display import Markdown
from dotenv import load_dotenv
import json
import time
import requests
from bs4 import BeautifulSoup
import datetime
from google.api_core import exceptions
# Para manejar errores de base de datos
from sqlalchemy.exc import SQLAlchemyError
from models.evento_reuniones import Evento
from config.dbconfig import session, Base, engine
from scripts.search import busqueda_eventos
from scripts.revisar_links import revisar_links

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
        f"Se trata de un evento en el ámbito de turismo de reuniones, congresos y convenciones.\n"
        f"Analiza el siguiente contenido de página web (texto e información aparente en imágenes o banners):\n\n"
        f"{contenido_web}\n\n"
        "Extrae y devuelve la siguiente información en formato JSON (sin envoltura Markdown):\n\n"
        "1. nombreEvento: Extrae el nombre oficial del evento exactamente como aparece en el título o encabezado principal, "
        "sin agregar numeración o texto adicional.\n\n"
        "2. tipoEvento: Identifica el tipo de evento y selecciona la opción que mejor se ajuste de la siguiente lista:\n"
        "   Asamblea, Conferencia, Congreso, Convención, Encuentro, Foro, Jornada, Seminario, Simposio, "
        "Exposición, Feria, Workshop, Evento Deportivo Internacional, Incentivo, Evento Cultural, Evento Deportivo Nacional, Otro tipo de evento.\n\n"
        "3. detalleTipoRotacion: Extrae el detalle de la rotación y selecciona la opción que mejor se ajuste de la siguiente lista:\n"
        "   Local, Provincial, Nacional - Regional (Patagonia), Nacional - Regional (NOA), Nacional - Regional (Litoral), "
        "Nacional - Regional (Centro), Nacional - Regional (Cuyo), Nacional, Internacional - Iberoamérica, Internacional - Panamérica, "
        "Internacional - Latinoamérica, Internacional - Sudamérica, Internacional - Mercosur, Internacional, Único, NS/NC.\n\n"
        "4. tema: Extrae el tema principal del evento y clasifícalo en la siguiente lista de temas:\n"
        "   Acuático, Agricultura y ganadería, Ajedrez, Alimentos, Arquitectura, Arte y diseño, Artes marciales y peleas, Automotores, "
        "Básquet, Bibliotecología, Ciclismo, Ciencias históricas y sociales, Ciencias naturales y exactas, Comercio, Comunicación, "
        "Cosmética y tratamientos estéticos, Cultura, Danza, Deporte y ocio, Derecho, Diseño de indumentaria y moda, Ecología y medio ambiente, "
        "Economía, Educación, Energía, Entretenimiento, parques y atracciones, Farmacia, Fisicoculturismo, Fútbol, Gastronomía, Geografia, "
        "Gobierno/Sindical, Golf, Handball, Hockey, Industria/Industrial, Lingüística, Literatura, Logística, Management y negocios, "
        "Maratón, Matemática y estadística, Medicina, Multideportes, Multisectorial, Ns/Nc, Odontología, Otro, Packaging y regalería, "
        "Polo, Psicología, Religión, Rugby, Seguridad, Seguros, Servicios, Sóftbol, Tecnología, Tenis, paddel o paleta, Tiro con arco y flecha, "
        "Transporte, Turismo y hotelería, Veterinaria, Vóley.\n\n"
        "5. fechaEdicion: Extrae la fecha de edición del artículo (generalmente al inicio del texto) y conviértela al formato AAAA-MM-DD.\n\n"
        "6. fechaInicio y fechaFinalizacion: Extrae de forma precisa las fechas en las que se realizará (o se realizó) el evento. En el artículo suele indicarse un rango, por ejemplo 'del 10 al 12 de julio'. Primero, fija el año en 2025 (es decir, si no se indica, siempre utiliza '2025') y devuelve ese valor en un campo llamado 'añoRaw'. Segundo, extrae el mes tal como aparece en la página (por ejemplo, 'julio') y devuélvelo en un campo 'mesLiteralRaw' sin modificarlo. Luego, extrae el día de inicio y el día final del rango, y devuelve estos valores en los campos 'diaInicioRaw' y 'diaFinalRaw', respectivamente. Con esa información, construye la fecha completa en formato AAAA-MM-DD para 'fechaInicio' y 'fechaFinalizacion'. Además, incluye un campo 'fechaRaw' que contenga la interpretación en crudo de la(s) fecha(s) tal como aparecen en la página. Ten en cuenta que los verbos pueden estar en presente o en pasado.\n\n"
        "7. Localidad: Extrae el nombre de la sede del evento (por ejemplo, 'Sede San Rafael', 'Instituto X', etc.) de forma precisa, "
        "ya que se utilizará para vincularla con un CSV de sedes.\n\n"
        "8. fechaRaw: Proporciona la interpretación en crudo de la(s) fecha(s) tal como aparecen en la página, sin procesar ni formatear.\n\n"
        "9. sedeRaw: Proporciona la información en crudo de la sede o localidad del evento, tal como se encuentra en la página, sin ningún procesamiento adicional.\n\n"
        "Devuélveme únicamente la información en formato JSON, sin etiquetas ni formateos adicionales."
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
        "Asamblea", "Conferencia", "Congreso", "Convención", "Encuentro", "Foro", "Jornada",
        "Seminario", "Simposio", "Exposición", "Feria", "Workshop", "Evento Deportivo Internacional",
        "Incentivo", "Evento Cultural", "Evento Deportivo Nacional", "Otro tipo de evento"
    ]
    for tipo in lista_tipos:
        if tipo.lower() in valor_extraido.lower():
            return tipo
    return "Otro tipo de evento"


def mapear_detalle_rotacion(valor_extraido):
    lista_rotacion = [
        "Local", "Provincial", "Nacional - Regional (Patagonia)", "Nacional - Regional (NOA)",
        "Nacional - Regional (Litoral)", "Nacional - Regional (Centro)", "Nacional - Regional (Cuyo)",
        "Nacional", "Internacional - Iberoamérica", "Internacional - Panamérica", "Internacional - Latinoamérica",
        "Internacional - Sudamérica", "Internacional - Mercosur", "Internacional", "Único", "NS/NC"
    ]
    for rotacion in lista_rotacion:
        if rotacion.lower() in valor_extraido.lower():
            return rotacion
    return "NS/NC"


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


def mapear_tema(valor_extraido):
    lista_temas = [
        "Acuático", "Agricultura y ganadería", "Ajedrez", "Alimentos", "Arquitectura",
        "Arte y diseño", "Artes marciales y peleas", "Automotores", "Básquet",
        "Bibliotecología", "Ciclismo", "Ciencias históricas y sociales", "Ciencias naturales y exactas",
        "Comercio", "Comunicación", "Cosmética y tratamientos estéticos", "Cultura", "Danza",
        "Deporte y ocio", "Derecho", "Diseño de indumentaria y moda", "Ecología y medio ambiente",
        "Economía", "Educación", "Energía", "Entretenimiento, parques y atracciones", "Farmacia",
        "Fisicoculturismo", "Fútbol", "Gastronomía", "Geografia", "Gobierno/Sindical", "Golf",
        "Handball", "Hockey", "Industria/Industrial", "Lingüística", "Literatura", "Logística",
        "Management y negocios", "Maratón", "Matemática y estadística", "Medicina", "Multideportes",
        "Multisectorial", "Ns/Nc", "Odontología", "Otro", "Packaging y regalería", "Polo",
        "Psicología", "Religión", "Rugby", "Seguridad", "Seguros", "Servicios", "Sóftbol",
        "Tecnología", "Tenis, paddel o paleta", "Tiro con arco y flecha", "Transporte",
        "Turismo y hotelería", "Veterinaria", "Vóley"
    ]
    for tema in lista_temas:
        if tema.lower() in valor_extraido.lower():
            return tema
    return valor_extraido


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
        'nombre': datos.get('nombreEvento', 'Desconocido'),
        'tipo': mapear_tipo_evento(datos.get('tipoEvento', '')),
        'detalle_tipo_rotacion': mapear_detalle_rotacion(datos.get('detalleTipoRotacion', '')),
        'tema': mapear_tema(datos.get('tema', '')),
        'fecha_edicion': formatear_fecha(datos.get('fechaEdicion')),
        'fecha_inicio': formatear_fecha(datos.get('fechaInicio')),
        'fecha_fin': formatear_fecha(datos.get('fechaFinalizacion')),
        'anio': datos.get('añoRaw', '2025'),  # Usar añoRaw del LLM
        # Usar mesLiteralRaw del LLM
        'mes': datos.get('mesLiteralRaw', 'Desconocido'),
        # Usar diaInicioRaw del LLM
        'dia_inicio': datos.get('diaInicioRaw', 'Desconocido'),
        # Usar diaFinalRaw del LLM
        'dia_fin': datos.get('diaFinalRaw', 'Desconocido'),
        # Usar fechaRaw del LLM
        'fecha_texto': datos.get('fechaRaw', 'Desconocida'),
        # Campos adicionales que no están en el modelo Evento, pero puedes querer en el DF temporal
        'sitioWeb': url,
        'categoria': "Académico",
        'frecuencia': "Anual",
        'agrupacion': "CONGRESOS Y CONVENCIONES",
        'provincia': "Mendoza",
        'entidadOrganizadora': "manual",
        # Mantenemos el campo raw de la sede
        'sedeRaw': datos.get('sedeRaw', 'Desconocido')
    }

    # El campo 'Localidad' del LLM se usa para buscar la 'localidad' final
    nombre_sede_extraido = datos.get('Localidad', '')
    procesado['localidad'] = buscar_localidad_sede(
        nombre_sede_extraido, sedes_df)

    return procesado


def formatear_fecha(fecha_str):
    if not fecha_str:
        return None  # Retorna None para columnas de fecha si no hay valor
    fecha_str = fecha_str.strip()
    if fecha_str.lower() in ["no proporcionada", "ns/nc", "null", ""]:
        return None  # Retorna None para columnas de fecha si el valor es inválido
    try:
        # Intenta si la fecha ya está en el formato AAAA-MM-DD
        dt = datetime.datetime.strptime(fecha_str, "%Y-%m-%d")
        return dt.date()  # Retorna solo la fecha
    except ValueError:
        pass
    # Intenta si la fecha tiene formato "dd/mm/AAAA"
    try:
        if "/" in fecha_str:
            dt = datetime.datetime.strptime(fecha_str, "%d/%m/%Y")
            return dt.date()
    except ValueError:
        pass
    # Si la fecha tiene formato en texto (por ejemplo, "1 de noviembre de 2025")
    if " de " in fecha_str:
        try:
            partes = fecha_str.split(" de ")
            if len(partes) >= 3:
                day = partes[0]
                month_text = partes[1].lower().strip()
                year = partes[2].split()[0]

                # Usar el año actual si no se especifica y está en 2025, o el año del texto
                if not year.isdigit():  # Si el año no es un número, asumir 2025
                    year_val = 2025
                else:
                    year_val = int(year)

                meses = {
                    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
                    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
                    "septiembre": "09", "setiembre": "09", "octubre": "10",
                    "noviembre": "11", "diciembre": "12"
                }
                mes_num = meses.get(month_text, "01")
                day_num = int(day)  # Convertir el día a entero

                return datetime.date(year_val, int(mes_num), day_num)
        except Exception as e:
            print(f"Error al parsear fecha '{fecha_str}': {e}")
            pass
    # Si no se pudo convertir, se retorna None para que SQLAlchemy inserte NULL
    return None


def limpiar_raw_response(raw_response):
    """
    Elimina delimitadores Markdown (triple backticks y etiquetas de lenguaje) de la respuesta.
    """
    if raw_response.startswith("```"):
        lines = raw_response.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return raw_response

def insertar_dataframe_en_mysql(df, session):
    print("\n--- Intentando insertar datos en la base de datos MySQL ---")
    if df.empty:
        print("El DataFrame está vacío. No hay datos para insertar.")
        return

    rows_inserted = 0
    for index, row in df.iterrows():
        try:
            evento = Evento(
                nombre=row['nombre'],
                tipo=row['tipo'],
                detalle_tipo_rotacion=row['detalle_tipo_rotacion'],
                tema=row['tema'],
                fecha_edicion=row['fecha_edicion'],
                fecha_inicio=row['fecha_inicio'],
                fecha_fin=row['fecha_fin'],
                anio=row['anio'],
                mes=row['mes'],
                dia_inicio=row['dia_inicio'],
                dia_fin=row['dia_fin'],
                fecha_texto=row['fecha_texto'],
                sede=row['sedeRaw'],
                sitio_web=row['sitioWeb']
            )

            session.add(evento)
            rows_inserted += 1

            session.commit()
            print(f"--> {rows_inserted} registros insertados y comiteados.")

        except SQLAlchemyError as e:
            session.rollback()  # Si hay un error, revierte la transacción actual
            print(f"Error al insertar registro {index}: {e}. Revirtiendo...")
            print(f"Datos del registro problemático: {row.to_dict()}")
            continue  # Continúa con el siguiente registro
        except Exception as e:
            print(f"Error inesperado al insertar registro {index}: {e}")
            print(f"Datos del registro problemático: {row.to_dict()}")
            continue

    try:
        session.commit()  # Comitear los registros restantes
        print(
            f"\n¡Inserción completada! Se insertaron un total de {rows_inserted} registros.")
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error final al comitear: {e}. Revirtiendo...")


# --- Flujo principal ---
if __name__ == '__main__':
    Base.metadata.create_all(engine)

    print("BUSCANDO EVENTOS")
    busqueda_eventos()

    print("REVISANDO LINKS")
    revisar_links()
    urls_df = pd.read_csv("./csvs/links_eventos_revisados.csv", sep=";", low_memory=False)
    lista_urls = urls_df["link"].to_list()

    sedes_df = pd.read_csv("./csvs/sedes.csv", sep=";")

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

    # Convertir la lista de diccionarios a DataFrame
    datos_eventos_filtrados = [
        evento for evento in datos_eventos if evento is not None]

    if datos_eventos_filtrados:
        df_eventos = pd.DataFrame(datos_eventos_filtrados)

        # --- Generar CSV (se mantiene como buena práctica) ---
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"./csvs/eventos_procesados_{timestamp}.csv"
        df_eventos.to_csv(output_filename, index=False, encoding='utf-8')
        print(
            f"¡Procesamiento completado! Datos guardados en '{output_filename}'")
        try:
            insertar_dataframe_en_mysql(df_eventos, session)
        finally:
            session.close()
    else:
        print("No se procesó ningún evento con éxito. El archivo CSV y la inserción en DB no fueron realizados.")
