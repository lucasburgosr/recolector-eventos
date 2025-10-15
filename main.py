import pandas as pd
import time
from datetime import datetime
from scripts.search import busqueda_eventos
from scripts.clasificar_eventos import extraer_datos_evento, guardar_eventos
from scripts.helpers_llm import extraer_contenido_web
from scripts.procesar_eventos import procesar_respuesta
from scripts.revisar_links import revisar_links
from scripts.correccion_sedes import corregir_sedes
from scripts.asignar_entidad import asignar_entidades_organizadoras
from config.dbconfig import session
from clients import cerebras_client, groq_client

if __name__ == '__main__':

    # Esta línea se necesita solo la primera vez para que se creen las tablas en
    # la DB.
    """ Base.metadata.create_all(engine) """
    
    print("Ejecutamos el main actual")

    # Obtenemos la lista de links y títulos en el archivo resultados_busqueda.csv
    busqueda_eventos()

    # Revisamos los links y generamos el archivo links_eventos_revisados.csv
    revisar_links(groq_client=groq_client)

    # Obtenemos los links revisados
    urls_df = pd.read_csv(
        "./data/links_eventos_revisados.csv", sep=";", low_memory=False)
    lista_urls = urls_df["link"].to_list()

    sedes_df = pd.read_csv("./data/sedes.csv", sep=";")
    datos_eventos = []

    # Este bloque procesa el evento obtenido de cada URL para sacar los campos que necesitamos
    for url in lista_urls:
        print(f"Procesando URL: {url}")
        contenido_web = extraer_contenido_web(url)
        if contenido_web:
            try:
                raw_response = extraer_datos_evento(
                    contenido_web, client=cerebras_client)

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

            except Exception as e:
                print(
                    f"Error inesperado durante el procesamiento de LLM para {url}: {e}")
        else:
            print(f"No se pudo extraer contenido de la URL {url}. Saltando...")
        time.sleep(7)

    datos_eventos_filtrados = [
        evento for evento in datos_eventos if evento is not None]

    df_organizaciones = pd.read_csv(
        "./data/organizadores_normalizado.csv", low_memory=False, sep=";")

    # Este bloque itera sobre los eventos procesados e intenta chequear sedes y organizadores
    # con un LLM + fuzzy matching. Por último, almacena todo en la base de datos y en un archivo CSV.
    if datos_eventos_filtrados:
        df_eventos = pd.DataFrame(datos_eventos_filtrados)

        df_eventos = corregir_sedes(
            df_eventos=df_eventos, df_sedes=sedes_df)

        df_eventos = asignar_entidades_organizadoras(
            df_eventos=df_eventos, df_organizaciones=df_organizaciones)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"./data/eventos_procesados_{timestamp}.csv"

        df_eventos.to_csv(output_filename, index=False,
                          encoding='utf-8', sep=";")
        print(
            f"¡Procesamiento completado! Datos guardados en '{output_filename}'")
        try:
            guardar_eventos(df_eventos, session)
        finally:
            session.close()
    else:
        print("No se procesó ningún evento con éxito. El archivo CSV y la inserción en DB no fueron realizados.")
