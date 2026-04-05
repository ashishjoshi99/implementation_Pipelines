from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "MedallionFramework") -> SparkSession:
    """
    Initializes and returns a Spark Session.
    If running locally, it configures it to run locally with Delta Spark.
    If running on Databricks, it will reuse the existing Spark cluster context.
    """
    # Using builder pattern. On Databricks, this inherently uses the existing cluster config.
    try:
        from delta import configure_spark_with_delta_pip
        
        builder = SparkSession.builder.appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") # Useful for SCD schema evolution
        
        # Configure for local testing, on Databricks these settings are typically overridden/ignored
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark
    except ImportError:
        # Fallback if testing locally without delta-spark installed (basic pyspark)
        # Note: Delta operations will fail if this is hit during execution
        print("Warning: delta-spark not found, using vanilla PySpark session.")
        return SparkSession.builder.appName(app_name).getOrCreate()
