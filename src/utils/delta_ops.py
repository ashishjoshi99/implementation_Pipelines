from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

def apply_overwrite(source_df: DataFrame, target_table: str):
    """Overwrites the target table entirely."""
    source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)


def apply_append(source_df: DataFrame, target_table: str):
    """Appends to the target table."""
    source_df.write.format("delta").mode("append").saveAsTable(target_table)


def apply_scd1(spark: SparkSession, source_df: DataFrame, target_table: str, join_keys: list):
    """
    Implements Slowly Changing Dimension Type 1 (Upsert).
    Updates existing records, inserts new ones.
    """
    if not spark.catalog.tableExists(target_table):
        # Initial write if table does not exist
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    delta_target = DeltaTable.forName(spark, target_table)
    
    # Construct merge condition natively
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in join_keys])
    
    delta_target.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()


def apply_scd2(spark: SparkSession, source_df: DataFrame, target_table: str, join_keys: list):
    """
    Implements Slowly Changing Dimension Type 2.
    Tracks historical data by setting is_active, effective_start_date, effective_end_date.
    """
    
    # 1. Prepare Source DataFrame with SCD2 columns for new records
    source_df = source_df \
        .withColumn("is_active", F.lit(True)) \
        .withColumn("effective_start_date", F.current_timestamp()) \
        .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
        
    if not spark.catalog.tableExists(target_table):
        # Initial write if table does not exist
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    delta_target = DeltaTable.forName(spark, target_table)
    target_df = delta_target.toDF()


    # 3. Detect changes by joining source and active target records
    staged_updates = source_df.alias("s") \
        .join(target_df.alias("t"), on=join_keys, how="inner") \
        .where("t.is_active = True") \
        .selectExpr("NULL as mergeKey", "s.*")

    staged_inserts = source_df.selectExpr(f"{join_keys[0]} as mergeKey", "*")
    
    # Union them to perform a single MERGE pass
    staged_data = staged_updates.unionByName(staged_inserts, allowMissingColumns=True)

    # 4. Perform the MERGE
    update_condition = " AND ".join([f"target.{key} = source.mergeKey" for key in join_keys]) + " AND target.is_active = True"
    
    delta_target.alias("target").merge(
        staged_data.alias("source"),
        update_condition
    ).whenMatchedUpdate(
        # Invalidate the old record
        set = {
            "is_active": F.lit(False),
            "effective_end_date": F.current_timestamp()
        }
    ).whenNotMatchedInsertAll(
        # Insert the new record
    ).execute()

WRITE_OPERATIONS = {
    "overwrite": apply_overwrite,
    "append": apply_append,
    "scd1": apply_scd1,
    "scd2": apply_scd2
}
