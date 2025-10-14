# === helpers_llm.py (o dejalo al tope de tu script) ===
import os
import time
import requests
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup
from groq import Groq, RateLimitError
from google.api_core import exceptions

MODELOS_GROQ_DEFAULT = [
    "openai/gpt-oss-20b",
    "openai/gpt-oss-120b",
    "llama-3.1-8b-instant",
    "llama-3.3-70b-versatile",
]

def extract_clean_text_from_url(
    url: str,
    timeout: float = 12.0,
    max_chars: int = 15000,
    max_lines: int = 1000,
) -> str:
    """
    Descarga HTML y devuelve texto limpio, truncado de forma segura.
    """
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text_content = soup.get_text(separator="\n")
    # Limitar líneas para evitar textos infinitos
    lines = (line.strip() for i, line in enumerate(text_content.splitlines()) if i < max_lines)
    compact = " ".join(lines)
    cleaned_text = "\n".join(chunk.strip() for chunk in compact.split("  ") if chunk.strip())
    if len(cleaned_text) > max_chars:
        cleaned_text = cleaned_text[:max_chars] + "\n... [Contenido truncado]"
    return cleaned_text

def llm_complete_with_failover(
    prompt: str,
    client: Groq,
    modelos: List[str] = None,
    max_retries_per_model: int = 1,
    base_backoff_seconds: float = 2.0,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Intenta completar con todos los modelos en todos los clientes (API keys) en orden.
    Reintenta por modelo ante 429 con backoff exponencial.
    Devuelve (content, modelo_usado, nombre_api_key) o (None, None, None).
    """
    modelos = modelos or MODELOS_GROQ_DEFAULT

    for model in modelos:
        for attempt in range(max_retries_per_model + 1):
            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                )
                content = resp.choices[0].message.content
                print(f"{model} respondió (len={len(content) if content else 0}).")
                return content, model

            except RateLimitError as e:
                print(f"[429] Rate limit en {model} (intento {attempt+1}/{max_retries_per_model+1}). {e}")
                if attempt < max_retries_per_model:
                    sleep_s = base_backoff_seconds * (2 ** attempt)
                    print(f" - Esperando {sleep_s:.1f}s y reintentando con {model}...")
                    time.sleep(sleep_s)
                    continue
                else:
                    print(f" - Agotados reintentos para {model}. Probando siguiente modelo...")
                    break

            except exceptions.ResourceExhausted as e:
                print(f"[Quota] Recurso agotado en:{model}: {e}. Probando siguiente modelo/clave...")
                break

            except Exception as e:
                print(f"[Error] {model} falló: {e}. Probando siguiente modelo/clave...")
                break

    print("[FAIL] Todas las combinaciones (key, modelo) fallaron o alcanzaron rate limit.")
    return None, None, None
