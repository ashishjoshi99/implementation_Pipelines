# Databricks notebook source
# COMMAND ----------

dbutils.widgets.text("config_file", "configs/sample_pipeline.yaml", "YAML Config Path")

# COMMAND ----------

import sys
import os

workspace_dir = os.path.dirname(os.path.abspath(__file__))
if workspace_dir not in sys.path:
    sys.path.append(workspace_dir)

from src.core.spark_session import get_spark_session
from src.core.pipeline_runner import run_pipelines

# COMMAND ----------

config_path = dbutils.widgets.get("config_file")

print(f"Running pipeline using configuration: {config_path}")

spark = get_spark_session("MedallionPipelineRunner")
run_pipelines(spark, config_path)
