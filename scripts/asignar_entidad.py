from fuzzywuzzy import process
import pandas as pd
from .helpers_llm import (
    build_groq_clients_from_env,
    extract_clean_text_from_url,
    llm_complete_with_failover,
    MODELOS_GROQ_DEFAULT,
)

def asignar_entidades_organizadoras(
    df_eventos: pd.DataFrame,
    df_organizaciones: pd.DataFrame,
    modelos=None,
    prefer_alt_key_first: bool = True,
    write_csv_path: str = "./data/eventos_con_entidades.csv",
) -> pd.DataFrame:
    """
    Para cada evento:
      1) Baja y limpia el HTML
      2) Pide al LLM la 'entidad organizadora principal' (texto)
      3) Fuzzy-match contra catálogo oficial
    Fallback entre modelos y entre API keys (EMETUR y GROQ).
    """
    entidades = df_organizaciones["Entidad organizadores"].dropna().unique().tolist()
    modelos = modelos or MODELOS_GROQ_DEFAULT
    clients = build_groq_clients_from_env(prefer_alt_first=prefer_alt_key_first)

    prompt_prefix = (
        "Esta página trata sobre un evento. Extraé el nombre de la entidad organizadora "
        "principal tal como aparece en el texto. No incluyas encabezados ni texto adicional.\n\n"
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

            entidad_raw = content.strip()
            best = process.extractOne(entidad_raw, entidades)
            mejor_match, score = (best if best else ("", 0))

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

            print(f"✔ [{index}] '{entidad_raw}' → '{entidad_final}' (score: {score}) [{used_key}:{used_model}]")

        except Exception as e:
            print(f"❌ Error en índice {index} (url={url}): {e}")
            df_eventos.at[index, "entidadOriginalLLM"] = "ERROR"
            df_eventos.at[index, "entidadOrganizadora"] = "ERROR"
            df_eventos.at[index, "matchScore"] = 0
            df_eventos.at[index, "requiereRevision"] = "Sí"

    df_eventos.to_csv(write_csv_path, sep=";", index=False)
    return df_eventos