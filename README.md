# Databricks PySpark Medallion Template

This project provides a plug-and-play template for defining Medallion Architecture pipelines (Bronze to Silver/Gold) purely via YAML configurations. It translates definitions into dynamically executed PySpark data transformations and Delta Lake merge operations without writing custom code for standard functions.

## Goal
To allow non-technical folks (Data Analysts, Business Users) to configure data pipelines intuitively while keeping performance scalable on Databricks clusters using Native Spark operations.

## Structure
- `configs/`: Stash your YAML configuration files here.
- `src/core/`: The underlying driver script and mock Spark session generator.
- `src/utils/`: Common transformations and SCD1/SCD2 writing algorithms.

## How to use

1. Define a YAML configuration (see `configs/sample_pipeline.yaml` for reference).
2. **In Databricks (Recommended for non-technical users):**
   - Open the `run_pipeline.py` notebook in Databricks.
   - Enter the path to your YAML configuration in the text box widget at the top.
   - Click "Run All". 
   - *To schedule this*: Create a Databricks Workflow (Job), select the `run_pipeline` notebook, and provide the `config_file` parameter directly in the Jobs UI.


## Available Configurations

### YAML Structure
```yaml
pipeline:
  name: "Pipeline Name"
  source:
    table: "catalog.schema.source_table_name"
  target:
    table: "catalog.schema.target_table_name"
  mode: "scd2" # Supported: overwrite, append, scd1, scd2
  join_keys:
    - "id" 
  transformations:
    - type: "transformation_name"
      # additional kwargs specific to transformation
```

### Supported Transformations
Transformations are registered in `src/utils/transformations.py`:
- `clean_column_names`: Lowers case and spaces-to-underscores. No column argument needed.
- `clean_email`: Trims and casts to lowercase. Pass `column: "column_name"`.
- `clean_phone`: Strips non-numeric values via Regex. Pass `column: "column_name"`.

*Want to add more? Add your PySpark function to `src/utils/transformations.py` and register it in `TRANSFORMATION_REGISTRY`!*
