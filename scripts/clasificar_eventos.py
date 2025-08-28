from groq import Groq, RateLimitError
from sqlalchemy.exc import SQLAlchemyError
from google.api_core import exceptions
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv
from IPython.display import Markdown
import textwrap
import google.generativeai as genai
import os
import sys
import pandas as pd
import json
import time

proyecto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(proyecto_dir)

from models.evento_reuniones import Evento
from config.dbconfig import session

load_dotenv()


def to_markdown(text):
    text = text.replace('•', '  *')
    return Markdown(textwrap.indent(text, '> ', predicate=lambda _: True))


GROQ_API_KEY = os.getenv("EMETUR_GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY)


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
        "10. agrupacion: Indica a que agrupación pertenece cada evento según tipoEvento. Así están compuestas las agrupaciones: \n"
        "CONGRESOS Y CONVENCIONES: Asamblea, Conferencia, Congreso, Convención, Encuentro, Foro, Jornada, Seminario, Simposio \n"
        "FERIAS Y EXPOSICIONES: Exposición, Feria, Workshop \n"
        "FUERA DEL ALCANCE DEL OETR: Evento Deportivo Internacional, Incentivo, Evento Cultural, Evento Deportivo Nacional, Otro tipo de evento"
        "Devuélveme únicamente la información en formato JSON, sin etiquetas ni formateos adicionales."
    )

    try:
        response = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            model="gemma2-9b-it"
        )
        print(response.choices[0].message.content)
        return response.choices[0].message.content
    except RateLimitError:
        raise
    except exceptions.ResourceExhausted:
        raise
    except Exception as e:
        print(f"Error al generar contenido con el modelo: {e}")
        return None


def guardar_eventos(df, session):
    print("\n--- Intentando insertar datos en la base de datos MySQL ---")
    if df.empty:
        print("El DataFrame está vacío. No hay datos para insertar.")
        return

    rows_inserted = 0
    for index, row in df.iterrows():
        try:
            evento_data = {
                'nombre': row.get('nombre'),
                'tipo': row.get('tipo'),
                'agrupacion': row.get('agrupacion'),
                'detalle_tipo_rotacion': row.get('detalle_tipo_rotacion'),
                'tema': row.get('tema'),
                'fecha_edicion': pd.to_datetime(row.get('fecha_edicion'), errors='coerce'),
                'fecha_inicio': pd.to_datetime(row.get('fecha_inicio'), errors='coerce'),
                'fecha_fin': pd.to_datetime(row.get('fecha_fin'), errors='coerce'),
                'anio': row.get('anio'),
                'mes': row.get('mes'),
                'dia_inicio': row.get('dia_inicio'),
                'dia_fin': row.get('dia_fin'),
                'fecha_texto': row.get('fecha_texto'),
                'sede': row.get('sedeRaw'),
                'sitio_web': row.get('sitio_web'),
                'entidad_organizadora': row.get('entidadOrganizadora'),
                'requiere_revision': row.get('requiereRevision')
            }
            evento = Evento(**evento_data)
            session.add(evento)
            rows_inserted += 1

        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error al procesar registro {index} para DB: {e}. Revirtiendo...")
            continue
        except Exception as e:
            print(f"Error inesperado al procesar registro {index}: {e}")
            continue
    
    try:
        session.commit()
        print(f"\n¡Inserción completada! Se insertaron un total de {rows_inserted} registros.")
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error final al comitear: {e}. Revirtiendo...")


def procesar_eventos_de_links():
    try:
        df_links = pd.read_csv("./data/links_eventos_revisados.csv", sep=";")
    except FileNotFoundError:
        print("Error: No se encontró el archivo 'links_eventos_revisados.csv'. Ejecute revisar_links.py primero.")
        return

    eventos_procesados = []
    output_filename = f"./data/eventos_procesados_{time.strftime('%Y%m%d_%H%M%S')}.csv"

    try:
        for index, row in df_links.iterrows():
            print(f"Procesando link {index + 1}/{len(df_links)}: {row['link']}")
            contenido_web = extraer_contenido_web(row['link'])
            if not contenido_web:
                continue

            datos_evento_str = extraer_datos_evento(contenido_web)
            if datos_evento_str:
                try:
                    if datos_evento_str.startswith("```json"):
                        datos_evento_str = datos_evento_str[7:-4].strip()
                    
                    datos_evento_json = json.loads(datos_evento_str)
                    datos_evento_json['sitioWeb'] = row['link']
                    eventos_procesados.append(datos_evento_json)
                except json.JSONDecodeError:
                    print(f"Error al decodificar JSON para el link {row['link']}")
                    print(f"Respuesta recibida: {datos_evento_str}")
                    continue
    
    except (RateLimitError, exceptions.ResourceExhausted):
        print("Límite de API alcanzado. Guardando los eventos procesados hasta ahora.")
    
    finally:
        if eventos_procesados:
            df_eventos = pd.DataFrame(eventos_procesados)
            df_eventos.to_csv(output_filename, sep=";", index=False)
            print(f"✅ Se guardaron {len(eventos_procesados)} eventos en {output_filename}")
            
            column_mapping = {
                'nombreEvento': 'nombre', 'tipoEvento': 'tipo', 'detalleTipoRotacion': 'detalle_tipo_rotacion',
                'tema': 'tema', 'fechaEdicion': 'fecha_edicion', 'fechaInicio': 'fecha_inicio',
                'fechaFinalizacion': 'fecha_fin', 'añoRaw': 'anio', 'mesLiteralRaw': 'mes',
                'diaInicioRaw': 'dia_inicio', 'diaFinalRaw': 'dia_fin', 'fechaRaw': 'fecha_texto',
                'sedeRaw': 'sedeRaw', 'sitioWeb': 'sitio_web'
            }
            df_eventos.rename(columns=column_mapping, inplace=True)
            
            if 'entidadOrganizadora' not in df_eventos.columns:
                df_eventos['entidadOrganizadora'] = None
            if 'requiereRevision' not in df_eventos.columns:
                df_eventos['requiereRevision'] = None

        else:
            print("No se procesaron eventos.")

df_eventos = pd.read_csv("./data/links_eventos_revisados.csv", sep=";", low_memory=False)
rows = []

for _, row in df_eventos.iterrows():
    try:
        contenido_web = extraer_contenido_web(row['link'])
        evento_clasificado = extraer_datos_evento(contenido_web)  # puede ser str JSON o dict

        data = json.loads(evento_clasificado) if isinstance(evento_clasificado, str) else evento_clasificado
        if not data:
            continue

        # data puede ser dict (una fila) o list[dict] (varias filas)
        if isinstance(data, dict):
            rows.append(data)
        elif isinstance(data, list):
            rows.extend(data)
        else:
            print("Formato no esperado:", type(data))
            continue

    except json.JSONDecodeError as e:
        print("JSON inválido:", e)
        continue
    except Exception as e:
        print("Error procesando link:", e)
        continue

df_clasificados = pd.json_normalize(rows) if rows else pd.DataFrame()
df_clasificados.to_csv("./data/eventos_clasificados.csv", sep=";", index=False)

    
