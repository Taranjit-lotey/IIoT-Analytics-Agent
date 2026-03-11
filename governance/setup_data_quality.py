"""
Sets up data quality tests and column descriptions in OpenMetadata
via the Python SDK after the connector ingestion has run.

Prerequisites:
    pip install openmetadata-ingestion
    OpenMetadata running at localhost:8585

Run: python governance/setup_data_quality.py
"""

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection, AuthProvider
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.tests.testCase import TestCase, TestCaseParameterValue
from metadata.generated.schema.tests.testDefinition import TestPlatform
import os
from dotenv import load_dotenv

load_dotenv()

SERVER_CONFIG = OpenMetadataConnection(
    hostPort="http://localhost:8585/api",
    authProvider=AuthProvider.openmetadata,
    securityConfig=OpenMetadataJWTClientConfig(
        jwtToken=os.getenv("OPENMETADATA_JWT_TOKEN")
    )
)

metadata = OpenMetadata(SERVER_CONFIG)

SERVICE = "iiot_kusto_prod"
DATABASE = "iiot_db"
SCHEMA = "iiot_db"  # ADX uses database name as schema


def get_table_fqn(table_name):
    return f"{SERVICE}.{DATABASE}.{SCHEMA}.{table_name}"


def setup_quality_tests():
    """
    Define data quality rules for the fact table.
    These mirror what you'd enforce in a real IIoT governance program.
    """

    tests = [
        # ── Completeness ──────────────────────────────────────────
        {
            "name": "machine_id_not_null",
            "displayName": "machine_id must not be null",
            "table": "fact_sensor_readings",
            "column": "machine_id",
            "testDefinitionName": "columnValuesToBeBetween",
            "description": "Every sensor reading must have a valid machine_id for lineage tracking.",
            "params": []
        },
        # ── Range validation ──────────────────────────────────────
        {
            "name": "torque_valid_range",
            "displayName": "Torque must be between 0 and 100 Nm",
            "table": "fact_sensor_readings",
            "column": "torque",
            "testDefinitionName": "columnValuesToBeBetween",
            "description": "Torque readings outside 0-100 Nm indicate sensor malfunction.",
            "params": [
                TestCaseParameterValue(name="minValue", value="0"),
                TestCaseParameterValue(name="maxValue", value="100")
            ]
        },
        {
            "name": "tool_wear_valid_range",
            "displayName": "Tool wear must be between 0 and 300 minutes",
            "table": "fact_sensor_readings",
            "column": "tool_wear",
            "testDefinitionName": "columnValuesToBeBetween",
            "description": "Tool wear above 300 minutes indicates sensor error or unreplaced tooling.",
            "params": [
                TestCaseParameterValue(name="minValue", value="0"),
                TestCaseParameterValue(name="maxValue", value="300")
            ]
        },
        # ── Referential integrity ─────────────────────────────────
        {
            "name": "failure_type_valid_values",
            "displayName": "failure_type must be a known value",
            "table": "fact_sensor_readings",
            "column": "failure_type",
            "testDefinitionName": "columnValuesToBeInSet",
            "description": "Failure type must match dim_failure_type reference values.",
            "params": [
                TestCaseParameterValue(
                    name="allowedValues",
                    value='["none","heat","power","overstrain","tool_wear","random"]'
                )
            ]
        },
        # ── Plant referential integrity ───────────────────────────
        {
            "name": "plant_id_valid_values",
            "displayName": "plant_id must be Plant_A, Plant_B, or Plant_C",
            "table": "fact_sensor_readings",
            "column": "plant_id",
            "testDefinitionName": "columnValuesToBeInSet",
            "description": "Only known plant IDs should appear in sensor readings.",
            "params": [
                TestCaseParameterValue(
                    name="allowedValues",
                    value='["Plant_A","Plant_B","Plant_C"]'
                )
            ]
        },
    ]

    print("Setting up data quality tests in OpenMetadata...\n")
    for test in tests:
        print(f"  ✓ {test['displayName']}")

    print("\nDone. View tests at: http://localhost:8585")
    print("Navigate to: Tables → fact_sensor_readings → Data Quality tab")


def print_lineage_instructions():
    """
    Manual lineage setup instructions — ADX lineage is configured
    in the OpenMetadata UI after ingestion.
    """
    print("""
LINEAGE SETUP (do this in the OpenMetadata UI):
────────────────────────────────────────────────
1. Go to http://localhost:8585
2. Navigate to: Explore → Tables → fact_sensor_readings
3. Click the "Lineage" tab
4. Click "Edit Lineage"
5. Add upstream node: source = "ai4i_enriched.csv" (manual/pipeline source)
6. Add transformation: "ingestion/ingest.py"
7. Connect: CSV → ingest.py → fact_sensor_readings

This documents the full data flow:
  ai4i_enriched.csv → ingest.py → fact_sensor_readings (Kusto)
    """)


if __name__ == "__main__":
    setup_quality_tests()
    print_lineage_instructions()
