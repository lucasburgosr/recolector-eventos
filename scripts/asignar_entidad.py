from fuzzywuzzy import process
from bs4 import BeautifulSoup
import requests
from groq import RateLimitError


def asignar_entidades_organizadoras(df_eventos, df_organizaciones, llm_client):
    entidades = df_organizaciones["Entidad organizadores"].dropna().unique().tolist()

    prompt = (
        "Esta página trata sobre un evento. Extraé el nombre de la entidad organizadora principal tal como aparece en el texto. "
        "No incluyas encabezados ni texto adicional. "
        "Este es el contenido de la página:\n\n"
    )

    for index, row in df_eventos.iterrows():
        try:
            url = row.get("sitioWeb", "")
            if not url or not isinstance(url, str):
                raise ValueError("URL inválida")

            response = requests.get(url, timeout=10)
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
                cleaned_text = cleaned_text[:max_chars] + "\n... [Contenido truncado]"

            llm_response = llm_client.chat.completions.create(
                model="gemma2-9b-it",
                messages=[{"role": "user", "content": prompt + cleaned_text}]
            )

            entidad_raw = llm_response.choices[0].message.content.strip()
            mejor_match, score = process.extractOne(entidad_raw, entidades)

            if score >= 90:
                entidad_final = mejor_match
                revision = "No"
            else:
                entidad_final = "NO_MATCH"
                revision = "Sí"

            df_eventos.at[index, "entidadOriginalLLM"] = entidad_raw
            df_eventos.at[index, "entidadOrganizadora"] = entidad_final
            df_eventos.at[index, "matchScore"] = score
            df_eventos.at[index, "requiereRevision"] = revision

            print(f"✔ [{index}] '{entidad_raw}' → '{entidad_final}' (score: {score})")

        except RateLimitError:
            print(f"Límite de API alcanzado en el índice {index}. Deteniendo el procesamiento.")
            break

        except Exception as e:
            print(f"❌ Error en índice {index}: {e}")
            df_eventos.at[index, "entidadOriginalLLM"] = "ERROR"
            df_eventos.at[index, "entidadOrganizadora"] = "ERROR"
            df_eventos.at[index, "matchScore"] = 0
            df_eventos.at[index, "requiereRevision"] = "Sí"
    
    df_eventos.to_csv("./data/eventos_con_entidades.csv", sep=";", index=False)

    return df_eventos

