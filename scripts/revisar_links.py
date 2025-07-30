from groq import Groq
import pandas as pd
from dotenv import load_dotenv
import os
import json

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def revisar_links():
    df = pd.read_csv("./csvs/resultados_busqueda.csv", sep=";", low_memory=False)
    lineas = []
    for _, row in df.iterrows():
        titulo = row['title']
        link = row['link']

        prompt = f"""Eres un asistente de revisión de links. Vas a revisar este link {link}
        que te voy a proporcionar para identificar si está relacionado a un evento en la provincia
        de Mendoza (Argentina). En caso de que sí, vas a devolver el título del evento ({titulo}) y
        el link en un objeto JSON con las propiedades "titulo" y "link". NO RESPONDAS NADA
        MÁS QUE LOS DATOS QUE TE SOLICITO, SOLO ESOS DOS CAMPOS. No agregues triple backtick ni la palabra 'json'"""

        response = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": f"{prompt}"
                }
            ],
            model="llama-3.3-70b-versatile",
            stream=False
        )

        contenido = response.choices[0].message.content.strip()

        try:
            data_json = json.loads(contenido)
            lineas.append(data_json)
        except json.JSONDecodeError:
            print(f"Error al parsear JSON en link: {link}")
            continue

    # Crear el DataFrame correctamente con columnas separadas
    datos = pd.DataFrame(lineas)
    datos.to_csv("./csvs/links_eventos_revisados.csv", sep=";", index=False)
