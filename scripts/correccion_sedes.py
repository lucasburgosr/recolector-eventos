import pandas as pd
from fuzzywuzzy import process
import requests
from bs4 import BeautifulSoup
from groq import RateLimitError

def corregir_sedes(df_eventos, df_sedes, llm_client, model_name="gemma2-9b-it"):
    """
    Extrae la sede principal desde el sitio del evento usando LLM y valida con fuzzy
    contra el catálogo oficial de sedes (df_sedes["Nombre"]).

    Columnas generadas en df_eventos:
      - sedeOriginalLLM: salida literal del LLM
      - sedeRaw_corregida: sede final asignada (match válido o 'NO_MATCH')
      - sedeMatchScore: score de fuzzy con el catálogo
      - sedeRequiereRevision: 'Sí' si no hubo match confiable o error
    """

    sedes_oficiales = df_sedes["Nombre"].dropna().unique().tolist()

    prompt_base = (
        "Esta página trata sobre un evento. Extraé el nombre de la sede o locación "
        "principal (por ejemplo, el teatro, centro cultural, estadio o sala) tal como "
        "aparece en el texto. No incluyas encabezados ni texto adicional.\n\n"
        "Este es el contenido de la página:\n\n"
    )

    for index, row in df_eventos.iterrows():
        try:
            url = row.get("sitioWeb", "")
            if not url or not isinstance(url, str):
                raise ValueError("URL inválida")

            # 1) Descargar y limpiar HTML
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()

            text_content = soup.get_text()
            # Limitar a primeras 1000 líneas para evitar ruido
            lines = (line.strip() for line_cnt, line in enumerate(text_content.splitlines()) if line_cnt < 1000)
            chunks = (phrase.strip() for phrase in " ".join(lines).split("  "))
            cleaned_text = "\n".join(chunk for chunk in chunks if chunk)

            # Truncar duro por seguridad
            max_chars = 15000
            if len(cleaned_text) > max_chars:
                cleaned_text = cleaned_text[:max_chars] + "\n... [Contenido truncado]"

            # 2) LLM: extraer sede principal literal
            llm_response = llm_client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt_base + cleaned_text}]
            )
            sede_raw = llm_response.choices[0].message.content.strip()

            # 3) Fuzzy matching contra catálogo oficial
            mejor_match, score = process.extractOne(sede_raw, sedes_oficiales)

            if score >= 90:
                sede_final = mejor_match
                revision = "No"
            else:
                sede_final = "NO_MATCH"
                revision = "Sí"

            # 4) Persistir resultados en el DF
            df_eventos.at[index, "sedeOriginalLLM"] = sede_raw
            df_eventos.at[index, "sedeRaw_corregida"] = sede_final  # mantiene el nombre de columna esperado
            df_eventos.at[index, "sedeMatchScore"] = score
            df_eventos.at[index, "sedeRequiereRevision"] = revision

            print(f"✔ [{index}] '{sede_raw}' → '{sede_final}' (score: {score})")

        except RateLimitError:
            print(f"Límite de API alcanzado en el índice {index}. Deteniendo el procesamiento.")
            break

        except Exception as e:
            print(f"❌ Error en índice {index}: {e}")
            df_eventos.at[index, "sedeOriginalLLM"] = "ERROR"
            df_eventos.at[index, "sedeRaw_corregida"] = "ERROR"
            df_eventos.at[index, "sedeMatchScore"] = 0
            df_eventos.at[index, "sedeRequiereRevision"] = "Sí"

    # 5) Guardar CSV final
    df_eventos.to_csv("./data/eventos_corregidos_sedes.csv", sep=";", index=False)
    return df_eventos
