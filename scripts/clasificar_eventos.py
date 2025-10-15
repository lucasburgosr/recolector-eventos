from sqlalchemy.exc import SQLAlchemyError
from cerebras.cloud.sdk import Cerebras
import os
import sys
import pandas as pd
from .helpers_llm import llamar_llm_con_fallback, MODELOS_CEREBRAS_DEFAULT

proyecto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(proyecto_dir)

from models.evento_reuniones import Evento

modelos = MODELOS_CEREBRAS_DEFAULT

def extraer_datos_evento(contenido_web: str, client: Cerebras | None = None, modelos: list[str] | None = None) -> str | None:
    """
    Orquesta el llamado al LLM con fallback de modelos.
    Devuelve el 'content' del LLM (string).
    """
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
        "11. categoria: Indica a que categoría pertenece cada evento. Estas son las opciones: Académico, Asociativo, Corporativo, Gubernamental"
        "Devuélveme únicamente la información en formato JSON, sin explicaciones, etiquetas ni formateos adicionales."
    )

    content, used_model = llamar_llm_con_fallback(
        prompt=prompt,
        client=client,
        modelos=modelos,
        max_retries_per_model=2,
        base_backoff_seconds=3.0,
    )

    if content is None:
        return None

    print(f"Respuesta cruda del LLM (modelo {used_model}):")
    print(content)
    return content


def guardar_eventos(df, session):
    """
    Mapea los valores de cada objeto JSON a la propiedad que le corresponde en el objeto
    Evento. Añade el evento y, en caso de no haber errores, continúa al commit.
    """
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
                'categoria': row.get('categoria'),
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