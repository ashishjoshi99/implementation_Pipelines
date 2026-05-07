import yaml
from pyspark.sql import DataFrame, SparkSession

from src.utils.transformations import TRANSFORMATION_REGISTRY
from src.utils.delta_ops import WRITE_OPERATIONS

def read_source(spark: SparkSession, source_cfg: dict) -> DataFrame:
    src_table = source_cfg.get('table')

    if not src_table:
        raise ValueError("Source configuration MUST contain a valid 'table' name (Unity Catalog).")
        
    return spark.table(src_table)

def apply_transformations(df: DataFrame, transformations: list) -> DataFrame:
    for t_config in transformations:
        t_type = t_config.pop('type', None)
        if t_type not in TRANSFORMATION_REGISTRY:
            raise ValueError(f"Transformation '{t_type}' is not registered.")
            
        transform_func = TRANSFORMATION_REGISTRY[t_type]
        df = transform_func(df, **t_config)
        
    return df

def write_target(spark: SparkSession, df: DataFrame, config: dict):
    target_cfg = config.get('target', {})
    tgt_table = target_cfg.get('table')
    
    if not tgt_table:
        raise ValueError("Target configuration MUST contain a valid 'table' name (Unity Catalog).")
        
    mode = config.get('mode', 'overwrite')
    join_keys = config.get('join_keys', [])

    if mode not in WRITE_OPERATIONS:
        raise ValueError(f"Write mode '{mode}' is not supported.")

    write_func = WRITE_OPERATIONS[mode]
    
    if mode in ['scd1', 'scd2']:
        if not join_keys:
            raise ValueError(f"Write mode '{mode}' requires 'join_keys' in the config.")
        write_func(spark, df, tgt_table, join_keys)
    else:
        write_func(df, tgt_table)

def run_pipelines(spark: SparkSession, config_path: str):
    with open(config_path, 'r') as file:
        config_data = yaml.safe_load(file)
        
    pipelines = config_data.get('pipelines', [])
    if not pipelines and 'pipeline' in config_data:
        pipelines = [config_data['pipeline']]
        
    if not pipelines:
        print("No pipelines found in configuration.")
        return

    for config in pipelines:
        print(f"\n{'='*40}")
        print(f"Starting pipeline: {config.get('name')}")
        print(f"{'='*40}")
        
        print("Reading source data...")
        source_cfg = config.get('source', {})
        df = read_source(spark, source_cfg)
        
        print("Applying transformations...")
        transformations = config.get('transformations', [])
        transformed_df = apply_transformations(df, transformations)
        
        print(f"Writing to target using mode: {config.get('mode')}")
        write_target(spark, transformed_df, config)
        
        print(f"Pipeline '{config.get('name')}' completed successfully!")
