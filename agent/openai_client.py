"""
Sends natural language questions to Azure AI Foundry (GPT-4o)
and returns a KQL query string.
"""

from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

# Load schema prompt once at import
SCHEMA_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "schema_prompt.txt")
with open(SCHEMA_PROMPT_PATH, "r") as f:
    SCHEMA_PROMPT = f.read()


def get_client():
    return AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version="2024-02-01",
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")  # e.g. https://yourresource.openai.azure.com/
    )


def nl_to_kql(question: str) -> str:
    """
    Takes a natural language question and returns a KQL query string.
    """
    client = get_client()

    response = client.chat.completions.create(
        model=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o"),  # your deployment name in AI Foundry
        messages=[
            {"role": "system", "content": SCHEMA_PROMPT},
            {"role": "user", "content": question}
        ],
        temperature=0,      # deterministic — we want consistent KQL, not creative answers
        max_tokens=500
    )

    kql = response.choices[0].message.content.strip()

    # Strip any accidental markdown backticks GPT adds
    if kql.startswith("```"):
        kql = kql.split("\n", 1)[-1]
    if kql.endswith("```"):
        kql = kql.rsplit("```", 1)[0]

    return kql.strip()
