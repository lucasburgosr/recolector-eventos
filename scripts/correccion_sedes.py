# === correccion_sedes.py (reemplazo) ===
import pandas as pd
from fuzzywuzzy import process
from .helpers_llm import (
    build_groq_clients_from_env,
    extract_clean_text_from_url,
    llm_complete_with_failover,
    MODELOS_GROQ_DEFAULT,
)

def corregir_sedes(
    df_eventos: pd.DataFrame,
    df_sedes: pd.DataFrame,
    modelos=None,
    prefer_alt_key_first: bool = True,
    write_csv_path: str = "./data/eventos_corregidos_sedes.csv",
) -> pd.DataFrame:
    """
    Pide al LLM que busque la sede principal en el texto y hace fuzzy contra
    las sedes oficiales. Si obtiene un score >= 90 asigna la sede, si no lo
    graba como NO_MATCH.
    """
    sedes_oficiales = df_sedes["Nombre"].dropna().unique().tolist()
    modelos = modelos or MODELOS_GROQ_DEFAULT
    clients = build_groq_clients_from_env(prefer_alt_first=prefer_alt_key_first)

    prompt_prefix = (
        "Esta página trata sobre un evento. Extraé el nombre de la sede o locación principal "
        "(teatro, centro cultural, estadio o sala) tal como aparece en el texto. "
        "No incluyas encabezados ni texto adicional.\n\n"
        "Contenido de la página:\n\n"
    )

    for index, row in df_eventos.iterrows():
        url = row.get("sitioWeb", "")
        try:
            if not url or not isinstance(url, str):
                raise ValueError("URL inválida")

            cleaned_text = extract_clean_text_from_url(url)
            prompt = prompt_prefix + cleaned_text

            content, used_model, used_key = llm_complete_with_failover(
                prompt=prompt,
                clients=clients,
                modelos=modelos,
                max_retries_per_model=1,
                base_backoff_seconds=2.0,
            )

            if not content:
                raise RuntimeError("LLM no devolvió contenido.")

            sede_raw = content.strip()
            best = process.extractOne(sede_raw, sedes_oficiales)
            mejor_match, score = (best if best else ("", 0))

            if score >= 90:
                sede_final = mejor_match
                revision = "No"
            else:
                sede_final = "NO_MATCH"
                revision = "Sí"

            df_eventos.at[index, "sedeOriginalLLM"] = sede_raw
            df_eventos.at[index, "sedeRaw_corregida"] = sede_final
            df_eventos.at[index, "sedeMatchScore"] = score
            df_eventos.at[index, "sedeRequiereRevision"] = revision

            print(f"✔ [{index}] '{sede_raw}' → '{sede_final}' (score: {score}) [{used_key}:{used_model}]")

        except Exception as e:
            print(f"❌ Error en índice {index} (url={url}): {e}")
            df_eventos.at[index, "sedeOriginalLLM"] = "ERROR"
            df_eventos.at[index, "sedeRaw_corregida"] = "ERROR"
            df_eventos.at[index, "sedeMatchScore"] = 0
            df_eventos.at[index, "sedeRequiereRevision"] = "Sí"

    df_eventos.to_csv(write_csv_path, sep=";", index=False)
    return df_eventos
