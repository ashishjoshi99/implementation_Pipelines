from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "MedallionFramework") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()
