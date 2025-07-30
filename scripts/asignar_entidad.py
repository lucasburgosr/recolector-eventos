from groq import Groq
import os
from dotenv import load_dotenv
import pandas as pd
from bs4 import BeautifulSoup
import requests

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

df = pd.read_csv("eventos_casi_final.csv", sep=";", low_memory=False)
df_organizaciones = pd.read_csv("entidades_fix.csv", sep=";", low_memory=False)

entidades = df_organizaciones["Entidad organizadores"].to_list()

prompt = f"""Analiza el contenido esta página web que trata sobre un evento para determinar
la entidad organizadora del mismo. Una vez realizada la evaluación, vas a responder únicamente
con la entidad. No agregues encabezado ni ningún texto explicativo.
Este es el contenido de la página: """

for index, row in df.iterrows():
    try:
        response = requests.get(row["sitioWeb"], timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for script_or_style in soup(["script", "style"]):
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

        llm_response = client.chat.completions.create(model="gemma2-9b-it", messages=[
            {
                "role": "user",
                "content": prompt + cleaned_text
            }
        ])

        entidad = llm_response.choices[0].message.content.strip()
        df.at[index, "entidadOrganizadora"] = entidad

        print(f"Entidad {entidad} actualizada exitosamente")

    except Exception as e:
        print(f"Error en índice {index}: {e}")
        df.at[index, "entidadOrganizadora"] = "ERROR"

df.to_csv("eventos_entidades_asignadas.csv", sep=";", index=False)
