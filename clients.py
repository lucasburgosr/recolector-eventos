from groq import Groq
from dotenv import load_dotenv
from cerebras.cloud.sdk import Cerebras
import os

load_dotenv()

groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))
cerebras_client = Cerebras(api_key=os.getenv("CEREBRAS_API_KEY"))