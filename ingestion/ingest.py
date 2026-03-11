"""
Ingests enriched AI4I dataset into Azure Data Explorer (Kusto DB).

Prerequisites:
1. Create a free ADX cluster at: https://dataexplorer.azure.com/freecluster
2. Create a database called: iiot_db
3. Run the KQL schema creation commands in kusto_schema.kql first
4. Set your environment variables (see .env.example)

Run: python ingestion/ingest.py
"""

import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, DataFormat
import os
from dotenv import load_dotenv

load_dotenv()

CLUSTER = os.getenv("KUSTO_CLUSTER")        # e.g. https://yourcluster.eastus2.kusto.windows.net
DATABASE = os.getenv("KUSTO_DATABASE")      # iiot_db
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT_ID = os.getenv("AZURE_TENANT_ID")


def get_kusto_client():
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        CLUSTER, CLIENT_ID, CLIENT_SECRET, TENANT_ID
    )
    return KustoClient(kcsb)


def get_ingest_client():
    ingest_cluster = CLUSTER.replace("https://", "https://ingest-")
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        ingest_cluster, CLIENT_ID, CLIENT_SECRET, TENANT_ID
    )
    return QueuedIngestClient(kcsb)


def ingest_fact_table():
    df = pd.read_csv("data/ai4i_enriched.csv")

    # Fact table: sensor readings
    fact_df = df[[
        "timestamp", "machine_id", "plant_id", "line_number",
        "air_temperature", "process_temperature", "rotational_speed",
        "torque", "tool_wear", "failure_type"
    ]]

    ingest_client = get_ingest_client()
    ingestion_props = IngestionProperties(
        database=DATABASE,
        table="fact_sensor_readings",
        data_format=DataFormat.CSV,
        ignore_first_record=True
    )

    # Save temp CSV and ingest
    fact_df.to_csv("/tmp/fact_sensor_readings.csv", index=False)
    ingest_client.ingest_from_file("/tmp/fact_sensor_readings.csv", ingestion_properties=ingestion_props)
    print(f"Ingested {len(fact_df)} rows into fact_sensor_readings")


def ingest_dim_machine():
    df = pd.read_csv("data/ai4i_enriched.csv")

    # Dim machine: unique machines
    dim_machine = df[["machine_id", "machine_type", "plant_id", "line_number"]].drop_duplicates()
    dim_machine["commissioned_date"] = "2023-01-01"

    ingest_client = get_ingest_client()
    ingestion_props = IngestionProperties(
        database=DATABASE,
        table="dim_machine",
        data_format=DataFormat.CSV,
        ignore_first_record=True
    )

    dim_machine.to_csv("/tmp/dim_machine.csv", index=False)
    ingest_client.ingest_from_file("/tmp/dim_machine.csv", ingestion_properties=ingestion_props)
    print(f"Ingested {len(dim_machine)} rows into dim_machine")


def ingest_dim_failure():
    failure_types = [
        {"failure_type": "none",       "severity": "none",     "description": "Normal operation, no failure detected"},
        {"failure_type": "heat",        "severity": "critical", "description": "Heat dissipation failure — process temp too high"},
        {"failure_type": "power",       "severity": "critical", "description": "Power failure — rotational speed/torque out of range"},
        {"failure_type": "overstrain",  "severity": "medium",   "description": "Overstrain failure — tool wear combined with torque"},
        {"failure_type": "tool_wear",   "severity": "medium",   "description": "Tool wear failure — tool wear exceeded threshold"},
        {"failure_type": "random",      "severity": "low",      "description": "Random failure — not attributable to specific cause"},
    ]

    import pandas as pd
    dim_failure = pd.DataFrame(failure_types)

    ingest_client = get_ingest_client()
    ingestion_props = IngestionProperties(
        database=DATABASE,
        table="dim_failure_type",
        data_format=DataFormat.CSV,
        ignore_first_record=True
    )

    dim_failure.to_csv("/tmp/dim_failure_type.csv", index=False)
    ingest_client.ingest_from_file("/tmp/dim_failure_type.csv", ingestion_properties=ingestion_props)
    print(f"Ingested {len(dim_failure)} rows into dim_failure_type")


if __name__ == "__main__":
    print("Starting ingestion...")
    ingest_fact_table()
    ingest_dim_machine()
    ingest_dim_failure()
    print("\nIngestion complete. Note: Queued ingestion may take 5-10 mins to appear in ADX.")
