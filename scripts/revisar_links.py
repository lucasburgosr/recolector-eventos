from groq import RateLimitError, APIStatusError
import pandas as pd
import time, json, sys, os
from groq import Groq
from helpers_llm import extraer_contenido_web, MODELOS_GROQ_DEFAULT
proyecto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(proyecto_dir)
from clients import groq_client

modelos_groq = MODELOS_GROQ_DEFAULT

def _guardar_progreso(index: int):
    with open("./data/progreso_revision_links.txt", 'w') as f:
        f.write(str(index))

contador_modelos = 0

def revisar_links(groq_client: Groq):
    """
    Itera sobre los links y, para cada uno, intenta usar los modelos de Groq en orden
    hasta que uno responda o todos fallen por TPD.
    Genera un archivo CSV con los links que pasaron el filtro.
    """
    try:
        df = pd.read_csv("./data/resultados_busqueda.csv", sep=";", low_memory=False)
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
                print(f"Reanudando desde el link número {start_index + 1}.")
    except (FileNotFoundError, ValueError):
        print("No se encontró archivo de progreso válido. Empezando desde el principio.")

    lineas = []
    modelos_agotados = set()

    for index, row in df.iloc[start_index:].iterrows():
        
        titulo = row['title']
        link = row['link']
        
        print(f"\n--- Procesando Link {index + 1}/{len(df)}: {link} ---")

        contenido_web = extraer_contenido_web(link)
        if contenido_web is None:
            _guardar_progreso(index + 1)
            continue

        prompt = ("Eres un asistente que revisa publicaciones en internet para encontrar "
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
                "En caso de que el evento **NO SEA VÁLIDO** responde solo 'No es válido'.")

        procesado_con_exito = False

        for modelo in modelos_groq:
            
            if modelo in modelos_agotados:
                print(f"[*] Modelo {modelo} ya está agotado. Saltando...")
                continue

            try:
                print(f"Intentando con el modelo: {modelo}")
                response = groq_client.chat.completions.create(
                    messages=[
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": f"Contenido web a revisar: {contenido_web}."}
                    ],
                    model=modelo,
                )

                contenido = response.choices[0].message.content.strip()

                if contenido != "No es válido":
                    try:
                        data_json = json.loads(contenido)
                        lineas.append(data_json)
                        print(f"[OK] Evento VÁLIDO encontrado con {modelo}.")
                    except json.JSONDecodeError:
                        print(f"[ERROR] La respuesta de {modelo} no fue un JSON válido: {contenido}")
                else:
                    print(f"[INFO] Evento NO válido según {modelo}.")

                procesado_con_exito = True
                break

            except RateLimitError as e:
                mensaje_error = str(e).lower()
                if "day" in mensaje_error or "daily" in mensaje_error:
                    print(f"[TPD] Límite diario alcanzado para {modelo}. Probando el siguiente.")
                    modelos_agotados.add(modelo)
                else:
                    print(f"[RATE LIMIT] Límite de RPM/TPM para {modelo}. Esperando...")
                    time.sleep(5)
            
            except APIStatusError as e:
                print(f"[ERROR API] Falló el modelo {modelo}: {e}. Probando el siguiente.")

        if not procesado_con_exito:
            print("Todos los modelos disponibles fallaron o están agotados. Guardando progreso y saliendo.")
            _guardar_progreso(index)
            break

        _guardar_progreso(index + 1)
        time.sleep(2)

    if lineas:
        datos = pd.DataFrame(lineas)
        datos.to_csv("./data/links_eventos_revisados.csv", sep=";", index=False, mode='a')
        print(f"----\nSe guardaron {len(lineas)} links revisados en ./data/links_eventos_revisados.csv\n----")
    else:
        print("----\nNo se encontraron nuevos links válidos para guardar.\n----")

revisar_links(groq_client=groq_client)
