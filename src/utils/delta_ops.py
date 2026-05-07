from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

def apply_overwrite(source_df: DataFrame, target_table: str):
    source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

def apply_append(source_df: DataFrame, target_table: str):
    source_df.write.format("delta").mode("append").saveAsTable(target_table)

def apply_scd1(spark: SparkSession, source_df: DataFrame, target_table: str, join_keys: list):
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    delta_target = DeltaTable.forName(spark, target_table)
    
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in join_keys])
    
    delta_target.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()


def apply_scd2(spark: SparkSession, source_df: DataFrame, target_table: str, join_keys: list):
    source_df = source_df \
        .withColumn("is_active", F.lit(True)) \
        .withColumn("effective_start_date", F.current_timestamp()) \
        .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
        
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    delta_target = DeltaTable.forName(spark, target_table)
    target_df = delta_target.toDF()


    staged_updates = source_df.alias("s") \
        .join(target_df.alias("t"), on=join_keys, how="inner") \
        .where("t.is_active = True") \
        .selectExpr("NULL as mergeKey", "s.*")

    staged_inserts = source_df.selectExpr(f"{join_keys[0]} as mergeKey", "*")
    
    staged_data = staged_updates.unionByName(staged_inserts, allowMissingColumns=True)

    update_condition = " AND ".join([f"target.{key} = source.mergeKey" for key in join_keys]) + " AND target.is_active = True"
    
    delta_target.alias("target").merge(
        staged_data.alias("source"),
        update_condition
    ).whenMatchedUpdate(
        set = {
            "is_active": F.lit(False),
            "effective_end_date": F.current_timestamp()
        }
    ).whenNotMatchedInsertAll(
    ).execute()

WRITE_OPERATIONS = {
    "overwrite": apply_overwrite,
    "append": apply_append,
    "scd1": apply_scd1,
    "scd2": apply_scd2
}
