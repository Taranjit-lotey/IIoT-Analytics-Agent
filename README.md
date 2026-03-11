# IIoT Analytics Platform — Predictive Maintenance

An analytics platform for industrial IoT sensor telemetry with data governance, built on Azure Data Explorer (Kusto), Azure AI Foundry, and OpenMetadata.

## Architecture

![Project Architecture](./images/architecture.png)

## Dataset

**AI4I 2020 Predictive Maintenance Dataset** (UCI Machine Learning Repository)  
10,000 synthetic industrial sensor readings with:
- Air/process temperature, rotational speed, torque, tool wear
- Failure types: heat, power, overstrain, tool_wear, random, none
- Enriched with: plant_id (Plant_A/B/C), line_number (1-5), timestamps

## Quick Start

### 1. Clone and install
```bash
git clone https://github.com/Taranjit-lotey/iiot-analytics-platform
cd iiot-platform
pip install -r requirements.txt
cp .env.example .env   # fill in your credentials
```

### 2. Set up Azure Data Explorer
- Create a free cluster at https://dataexplorer.azure.com/freecluster
- Create database: `iiot_db`
- Run `ingestion/kusto_schema.kql` in the ADX query editor

### 3. Prepare and ingest data
```bash
# Download AI4I dataset from Kaggle → save as data/ai4i2020.csv
python data/enrich_dataset.py
python ingestion/ingest.py
```

### 4. Run queries
```bash
cd query
python query.py
```

### 5. Start OpenMetadata (data governance)
```bash
docker compose up -d
# Open http://localhost:8585 (admin/admin)
# Run connector: metadata ingest -c governance/openmetadata_config.yml
python governance/setup_data_quality.py
```

## Example Queries

```
You: which plant had the most failures last week?
You: show me machines with tool wear above 200
You: what is the average torque on line 3?
You: how many heat failures happened per day this month?
You: which line has the highest failure rate?
You: compare failure counts across all plants
You: show me the last 10 power failures
```

Type `kql` to see the generated query for any result.

## Data Governance (OpenMetadata)

- **Data catalog** — all 3 Kusto tables registered with column descriptions and ownership
- **Data quality tests** — null checks, range validation (torque, tool_wear), referential integrity (failure_type, plant_id)
- **Lineage** — documented flow from CSV source → ingestion script → Kusto fact table

## Stack

| Component | Technology |
|-----------|-----------|
| Time-series database | Azure Data Explorer (Kusto DB) |
| Query language | KQL (Kusto Query Language) |
| Query interpretation | Azure AI Foundry — GPT-4o |
| Data governance | OpenMetadata |
| Ingestion | Python, azure-kusto-ingest |
| Interface | Terminal (Python) |