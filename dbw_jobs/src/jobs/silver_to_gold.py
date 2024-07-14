from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr, row_number, date_trunc
from pyspark.sql.types import StringType, IntegerType, TimestampType, DateType, LongType
from pyspark.dbutils import DBUtils
import os
import argparse
import yaml
import sys

# Global variables
spark = None
dbutils = None


def get_metadata(source: str, env: str):
    base_path = os.environ.get('METADATA_PATH', '../..')
    metadata_path = os.path.join(base_path, 'metadata', 'sources', source, f"{source}.yaml")
    with open(metadata_path, 'r') as file:
        return yaml.safe_load(file)

def read_from_silver(metadata, env):
    silver_path = metadata['silver_path'].replace("${env}", env).replace("${source}", metadata['source_name'])
    return spark.read.format("delta").load(silver_path)

def transform_to_gold(df, metadata):
    gold_schema = metadata['schema']['gold']
    select_expr = []
    
    for field in gold_schema:
        if 'transform_expr' in field:
            select_expr.append(expr(field['transform_expr']).alias(field['name']))
        elif 'source_col' in field:
            select_expr.append(col(field['source_col']).alias(field['name']))
        else:
            select_expr.append(col(field['name']))
    
    df_gold = df.select(*select_expr)
    return df_gold

def get_key_columns(metadata):
    # Check if key_columns is defined in the metadata
    if 'key_columns' in metadata:
        return metadata['key_columns'].get('gold', [])
    # If not, assume the first column in the gold schema is the key
    return [metadata['schema']['gold'][0]['name']]

def write_to_gold(df, metadata, env):
    gold_path = metadata['gold_path'].replace("${env}", env).replace("${source}", metadata['source_name'])
    
    # Apply partitioning if specified
    if 'partitioning' in metadata and 'gold' in metadata['partitioning']:
        partition_info = metadata['partitioning']['gold'][0]  # Assuming we're using the first partition specification
        partition_col = partition_info['column']
        granularity = partition_info['granularity']
        
        # Check the data type of the partition column
        partition_col_type = df.schema[partition_col].dataType
        
        if isinstance(partition_col_type, TimestampType) or isinstance(partition_col_type, DateType):
            # For timestamp or date columns, apply time-based partitioning
            if granularity == 'day':
                df = df.withColumn(f"{partition_col}_partitioned", date_trunc('day', col(partition_col)))
            elif granularity == 'month':
                df = df.withColumn(f"{partition_col}_partitioned", date_trunc('month', col(partition_col)))
            elif granularity == 'year':
                df = df.withColumn(f"{partition_col}_partitioned", date_trunc('year', col(partition_col)))
            else:
                raise ValueError(f"Unsupported granularity for time-based column: {granularity}")
        elif isinstance(partition_col_type, IntegerType) or isinstance(partition_col_type, LongType):
            # For integer columns, we might want to partition by ranges
            if granularity.isdigit():
                range_size = int(granularity)
                df = df.withColumn(f"{partition_col}_partitioned", (col(partition_col) / range_size).cast("int") * range_size)
            else:
                raise ValueError(f"Unsupported granularity for integer column: {granularity}")
        elif isinstance(partition_col_type, StringType):
            # For string columns, we might want to partition by the first few characters
            if granularity.isdigit():
                prefix_length = int(granularity)
                df = df.withColumn(f"{partition_col}_partitioned", expr(f"substring({partition_col}, 1, {prefix_length})"))
            else:
                raise ValueError(f"Unsupported granularity for string column: {granularity}")
        else:
            # For other types, we might just use the column as-is
            df = df.withColumn(f"{partition_col}_partitioned", col(partition_col))

    # Get key columns from metadata
    key_columns = get_key_columns(metadata)

    # Deduplicate the dataframe based on key columns
    window = Window.partitionBy(*key_columns).orderBy(col("lastUpdated").desc())
    df_deduplicated = df.withColumn("row_number", row_number().over(window)) \
                        .filter(col("row_number") == 1) \
                        .drop("row_number")

    # Check if the Delta table already exists
    if DeltaTable.isDeltaTable(spark, gold_path):
        # If it exists, use Delta's merge operation to update
        deltaTable = DeltaTable.forPath(spark, gold_path)
        
        # Construct merge condition based on key columns
        merge_condition = " AND ".join([f"oldData.{col} = newData.{col}" for col in key_columns])
        
        # Include partition column in the merge condition if it exists
        if partition_col:
            merge_condition += f" AND oldData.{partition_col} = newData.{partition_col}"
        
        # Construct update statement
        update_stmt = {f"oldData.{col.name}": f"newData.{col.name}" for col in df_deduplicated.schema}
        
        # Construct insert statement
        insert_stmt = {col.name: f"newData.{col.name}" for col in df_deduplicated.schema}
        
        # Perform merge operation
        deltaTable.alias("oldData").merge(
            df_deduplicated.alias("newData"),
            merge_condition
        ).whenMatchedUpdate(set=update_stmt
        ).whenNotMatchedInsert(values=insert_stmt
        ).execute()
    else:
        # If it doesn't exist, write as a new Delta table
        write_options = {
            "format": "delta",
            "mode": "overwrite",
            "overwriteSchema": "true"
        }
        
        if partition_col:
            write_options["partitionBy"] = partition_col

        df_deduplicated.write.options(**write_options).save(gold_path)

    print(f"Data written to Gold layer: {gold_path}")

def process_data(metadata, env):
    df_silver = read_from_silver(metadata, env)
    df_gold = transform_to_gold(df_silver, metadata)
    write_to_gold(df_gold, metadata, env)
    return df_gold

def main(source=None, env=None):
    if source is None or env is None:
        print("Usage: bronze_to_silver --source <source> --env <environment>")
        sys.exit(1)
    
    metadata = get_metadata(source, env)
    print(f"Loaded metadata for source: {source}, environment: {env}")

    df_gold = process_data(metadata, env)
    
    print(f"Gold layer processing completed. Number of records: {df_gold.count()}")
    

def cli_main():
    global spark, dbutils
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="The source Data")
    parser.add_argument("--env", required=True, help="Environment (dev, prod, etc.)")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()
    dbutils = DBUtils(spark)
    # Set the storage account key in Spark configuration
    spark.conf.set(
        "fs.azure.account.key.syphaxstoragedev.dfs.core.windows.net",
        dbutils.secrets.get(scope="adls-scope", key="adls-key")
    )
    
    main(args.source, args.env)

if __name__ == "__main__":
    cli_main()