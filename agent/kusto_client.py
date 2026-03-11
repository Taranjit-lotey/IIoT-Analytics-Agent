"""
Handles all communication with Azure Data Explorer (Kusto DB).
"""

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def get_client():
    cluster = os.getenv("KUSTO_CLUSTER")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    tenant_id = os.getenv("AZURE_TENANT_ID")

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        cluster, client_id, client_secret, tenant_id
    )
    return KustoClient(kcsb)


def run_query(kql: str) -> pd.DataFrame:
    """
    Executes a KQL query against iiot_db and returns a DataFrame.
    Raises ValueError on bad queries, KustoServiceError on ADX errors.
    """
    database = os.getenv("KUSTO_DATABASE", "iiot_db")
    client = get_client()

    try:
        response = client.execute(database, kql)
        rows = response.primary_results[0]

        # Convert to DataFrame
        columns = [col.column_name for col in rows.columns]
        data = [list(row) for row in rows]
        df = pd.DataFrame(data, columns=columns)
        return df

    except KustoServiceError as e:
        raise ValueError(f"KQL execution error: {str(e)}")
