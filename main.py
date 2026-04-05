import argparse
import sys
import os

# Add the project root to the sys path so imports work correctly when running directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.spark_session import get_spark_session
from src.core.pipeline_runner import run_pipelines

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Medallion Architecture Pipeline")
    parser.add_argument(
        "--config", 
        type=str, 
        required=True, 
        help="Path to the YAML configuration file"
    )
    
    args = parser.parse_args()
    
    # Initialize the Spark Session
    spark = get_spark_session("MedallionPipelineRunner")
    
    # Execute pipelines
    run_pipelines(spark, args.config)
