import pandas as pd
import time
from datetime import datetime
from config.dbconfig import Base, engine, session
from scripts.search import busqueda_eventos
from scripts.clasificar_eventos import extraer_contenido_web, extraer_datos_evento, guardar_eventos, client
from scripts.procesar_eventos import procesar_respuesta
from scripts.revisar_links import revisar_links
from scripts.correccion_sedes import corregir_sedes
from scripts.asignar_entidad import asignar_entidades_organizadoras

if __name__ == '__main__':

    Base.metadata.create_all(engine)

    # Obtenemos la lista de links y títulos en el archivo resultados_busqueda.csv
    busqueda_eventos()

    # Revisamos los links y generamos el archivo links_eventos_revisados.csv
    revisar_links()

    # Obtenemos los links revisados
    urls_df = pd.read_csv(
        "./data/links_eventos_revisados.csv", sep=";", low_memory=False)
    lista_urls = urls_df["link"].to_list()

    sedes_df = pd.read_csv("./data/sedes.csv", sep=";")
    datos_eventos = []

    # Procesamiento de las URLS con LLM
    for url in lista_urls:

        print(f"Procesando URL: {url}")
        contenido_web = extraer_contenido_web(url)
        if contenido_web:
            try:
                raw_response = extraer_datos_evento(contenido_web)

                if raw_response == "NO_HAY_MODELOS_DISPONIBLES":
                    print(
                        "Todos los modelos alcanzaron el límite de requests gratuitas. Deteniendo el procesamiento.")
                    break

                elif raw_response:
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
    
    df_organizaciones = pd.read_csv("./data/organizadores_normalizado.csv", low_memory=False, sep=";")

    if datos_eventos_filtrados:
        df_eventos = pd.DataFrame(datos_eventos_filtrados)
        
        # Corregimos las sedes usando fuzzy matching
        df_eventos = corregir_sedes(df_eventos=df_eventos, df_sedes=sedes_df)
        
        df_eventos = asignar_entidades_organizadoras(df_eventos=df_eventos, df_organizaciones=df_organizaciones, llm_client=client)

        # Generamos el CSV con los eventos para usar en la carga posteriormente
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"./data/eventos_procesados_{timestamp}.csv"
        
        df_eventos.to_csv(output_filename, index=False, encoding='utf-8', sep=";")
        print(
            f"¡Procesamiento completado! Datos guardados en '{output_filename}'")
        try:
            guardar_eventos(df_eventos, session)
        finally:
            session.close()
    else:
        print("No se procesó ningún evento con éxito. El archivo CSV y la inserción en DB no fueron realizados.")
