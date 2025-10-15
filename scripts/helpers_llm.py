import time
import requests
from typing import List, Tuple, Optional, Any
from bs4 import BeautifulSoup

MODELOS_GROQ_DEFAULT = [
    "openai/gpt-oss-20b",
    "openai/gpt-oss-120b",
    "llama-3.1-8b-instant",
    "llama-3.3-70b-versatile",
]

MODELOS_CEREBRAS_DEFAULT = [
    "gpt-oss-120b", "llama-3.3-70b", "llama3.1-8b", "llama-4-scout-17b-16e-instruct"
]


def extraer_contenido_web(url: str) -> str | None:
    """
    Extrae el contenido textual principal de una URL de forma inteligente.

    Busca en orden jerárquico las etiquetas <main>, <article> y, como último
    recurso, el <body> para aislar el contenido relevante y descartar
    menús, barras laterales y pies de página.
    """
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for element in soup(['script', 'style', 'nav', 'footer', 'aside']):
            element.decompose()

        if soup.main:
            content_container = soup.main
        elif soup.article:
            content_container = soup.article
        elif soup.find('div', {'id': 'content'}):
            content_container = soup.find('div', {'id': 'content'})
        elif soup.find('div', {'class': 'content'}):
            content_container = soup.find('div', {'class': 'content'})
        else:
            content_container = soup.body

        if not content_container:
            return None

        cleaned_text = content_container.get_text(separator=' ', strip=True)

        max_chars = 15000
        if len(cleaned_text) > max_chars:
            print(
                f"    -> Contenido principal aún es largo ({len(cleaned_text)}). Truncando.")
            cleaned_text = cleaned_text[:max_chars] + \
                "\n... [Contenido principal truncado]"

        return cleaned_text

    except requests.exceptions.RequestException as e:
        print(f"Error de red al acceder a la URL {url}: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado al procesar el contenido de {url}: {e}")
        return None


def llamar_llm_con_fallback(
    prompt: str,
    client: Any,
    modelos: List[str],
    max_retries_per_model: int = 2,
    base_backoff_seconds: float = 3.0,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Intenta obtener una completion de un LLM usando una lista de modelos.

    Maneja fallos y rate limits de forma inteligente:
    - Si el error es por un límite diario (TPD), salta inmediatamente al siguiente modelo.
    - Si el error es a corto plazo (TPM/RPM), reintenta con backoff exponencial.
    - Ante otros errores, pasa al siguiente modelo.

    Return:
        Una tupla (contenido_respuesta, modelo_usado) o (None, None) si todo falla.
    """
    for model in modelos:
        for attempt in range(max_retries_per_model + 1):
            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                )
                content = resp.choices[0].message.content
                print(f"✅ [OK] Modelo '{model}' respondió exitosamente.")
                return content, model

            except Exception as e:
                if "RateLimitError" in type(e).__name__:
                    error_message = str(e).lower()
                    if "day" in error_message or "daily" in error_message:
                        print(
                            f"[TPD] Límite diario alcanzado para '{model}'. Saltando al siguiente modelo.")
                        break
                    print(
                        f"[429] Rate limit en '{model}' (intento {attempt+1}/{max_retries_per_model+1}).")
                    if attempt < max_retries_per_model:
                        sleep_s = base_backoff_seconds * (2 ** attempt)
                        print(f"Esperando {sleep_s:.1f}s para reintentar...")
                        time.sleep(sleep_s)
                        continue
                    else:
                        print(f"Reintentos agotados para '{model}'.")
                        break

                else:
                    print(f"Fallo en '{model}': {type(e).__name__} - {e}.")
                    print("Pasando al siguiente modelo...")
                    break

    print("Todos los modelos disponibles fallaron o alcanzaron sus límites.")
    return None, None
