import json
from datetime import datetime


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
    """
    Mapea los campos de la respuesta cruda a los campos del objeto JSON Evento.
    """
    raw_response = limpiar_raw_response(raw_response)

    try:
        datos = json.loads(raw_response)
    except json.JSONDecodeError as e:
        print(f"Error procesando JSON para {url}: {e}")
        print(f"Respuesta cruda que causó el error: {raw_response}")
        return None

    procesado = {
        'nombre': datos.get('nombreEvento', 'Desconocido'),
        'tipo': mapear_tipo_evento(datos.get('tipoEvento', '')),
        'detalle_tipo_rotacion': mapear_detalle_rotacion(datos.get('detalleTipoRotacion', '')),
        'tema': mapear_tema(datos.get('tema', '')),
        'fecha_edicion': formatear_fecha(datos.get('fechaEdicion')),
        'fecha_inicio': formatear_fecha(datos.get('fechaInicio')),
        'fecha_fin': formatear_fecha(datos.get('fechaFinalizacion')),
        'anio': datos.get('añoRaw', '2025'),
        'mes': datos.get('mesLiteralRaw', 'Desconocido'),
        'dia_inicio': datos.get('diaInicioRaw', 'Desconocido'),
        'dia_fin': datos.get('diaFinalRaw', 'Desconocido'),
        'fecha_texto': datos.get('fechaRaw', 'Desconocida'),
        'sitioWeb': url,
        'categoria': "Académico",
        'frecuencia': "Anual",
        'agrupacion': datos.get('agrupacion', 'Desconocido'),
        'provincia': "Mendoza",
        'entidadOrganizadora': "-",
        # Mantenemos el campo raw de la sede
        'sedeRaw': datos.get('sedeRaw', 'Desconocido')
    }

    # El campo 'Localidad' del LLM se usa para buscar la 'localidad' final
    nombre_sede_extraido = datos.get('Localidad', '')
    procesado['localidad'] = buscar_localidad_sede(
        nombre_sede_extraido, sedes_df)

    return procesado


def formatear_fecha(fecha_str):
    """
    Estandariza las fechas en formato YYYY/MM/DD
    """
    if not fecha_str:
        return None
    fecha_str = fecha_str.strip()
    if fecha_str.lower() in ["no proporcionada", "ns/nc", "null", ""]:
        return None
    try:
        dt = datetime.strptime(fecha_str, "%Y-%m-%d")
        return dt.date()
    except ValueError:
        pass
    try:
        if "/" in fecha_str:
            dt = datetime.strptime(fecha_str, "%d/%m/%Y")
            return dt.date()
    except ValueError:
        pass
    # Si la fecha tiene formato en texto:
    if " de " in fecha_str:
        try:
            partes = fecha_str.split(" de ")
            if len(partes) >= 3:
                day = partes[0]
                month_text = partes[1].lower().strip()
                year = partes[2].split()[0]

                # Usar el año actual si no se especifica
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
                day_num = int(day)

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
