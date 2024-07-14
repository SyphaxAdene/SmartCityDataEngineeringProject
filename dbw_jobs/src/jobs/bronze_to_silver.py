from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, date_trunc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType, LongType, DateType
import os
import argparse
from pyspark.dbutils import DBUtils
import yaml
import sys
from validation.validator import DataValidator
from importlib.resources import files
from confluent_kafka import Consumer
import json
from dateutil.parser import parse
import time


# Global variables
spark = None
dbutils = None

def get_metadata(source: str, env: str):
    base_path = os.environ.get('METADATA_PATH', '../../..')
    metadata_path = os.path.join(base_path, 'metadata', 'sources', source, f"{source}.yaml")
    with open(metadata_path, 'r') as file:
        return yaml.safe_load(file)

def get_secret_from_path(full_secret_path):
    parts = full_secret_path.split('.')
    if len(parts) != 3:
        raise ValueError(f"Invalid secret path: {full_secret_path}")
    return dbutils.secrets.get(scope=parts[1], key=parts[2])

def read_from_kafka(metadata):
    kafka_config = metadata['source']['kafka']
    
    brokers = get_secret_from_path(kafka_config['brokers'])
    topic = get_secret_from_path(kafka_config['topic'])
    sasl_username = get_secret_from_path(kafka_config['options']['sasl_username'])
    sasl_password = get_secret_from_path(kafka_config['options']['sasl_password'])
    
    # Kafka configuration parameters
    conf = {
        'bootstrap.servers': brokers,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': sasl_username,
        'sasl.password': sasl_password,
        'group.id': 'my-consumer-a5',
        'auto.offset.reset': 'earliest'
    }

    def consume_messages(max_messages, timeout):
        consumer = Consumer(conf)
        consumer.subscribe([topic])
        
        messages = []
        try:
            while len(messages) < max_messages:
                msg = consumer.poll(timeout)
                if msg is None:
                    break
                message = json.loads(msg.value().decode('utf-8'))
                # Parse timestamp fields
                for field in schema.fields:
                    if isinstance(field.dataType, TimestampType) and field.name in message:
                        message[field.name] = parse(message[field.name])
                messages.append(message)
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            consumer.close()
        
        return messages

    schema = create_struct_type(metadata['schema']['bronze'])
    # Consume messages
    messages = consume_messages(max_messages=300, timeout=10)
    print("messages --> ",messages)
    print("schema --> ",schema)
    # Convert to a Spark DataFrame
    df = spark.createDataFrame(messages, schema)

    # Display the DataFrame
    print("df --->")
    df.show()
    return df

def read_from_bronze(metadata, env):
    bronze_path = metadata['bronze_path'].replace("${env}", env).replace("${source}", metadata['source_name'])
    schema = create_struct_type(metadata['schema']['bronze'])
    
    file_format = metadata['source'].get('file_format', 'json').lower()
    
    if file_format == 'json':
        df = spark.read.json(bronze_path, schema=schema)
    elif file_format == 'csv':
        df = spark.read.csv(bronze_path, schema=schema, header=True)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    return df

def create_struct_type(fields):
    return StructType([create_struct_field(field) for field in fields])

def create_struct_field(field):
    field_type = {
        'string': StringType(),
        'double': DoubleType(),
        'integer': IntegerType(),
        'long': LongType(),
        'boolean': BooleanType(),
        'timestamp': TimestampType()
    }.get(field['type'], StringType())

    if field['type'] == 'struct':
        field_type = create_struct_type(field['fields'])

    return StructField(field['name'], field_type, field.get('nullable', True))

def enrich_data(df, metadata):
    silver_schema = metadata['schema']['silver']
    
    for field in silver_schema:
        if 'transform_expr' in field:
            df = df.withColumn(field['name'], expr(field['transform_expr']))
        elif 'source_col' in field:
            df = df.withColumn(field['name'], col(field['source_col']))
    
    # add the technical column "time of processing"
    df = df.withColumn("processedTimestamp", current_timestamp())
    # select only the silver columns
    df = df.select([field['name'] for field in silver_schema])
    return df

def validate_data(df, metadata):
    rules_file = files('validation.rules').joinpath(f"{metadata['source_name']}_validation_rules.yaml")
    validator = DataValidator(str(rules_file))
    return validator.validate(df)

def write_to_silver(df, metadata, env):
    silver_path = metadata['silver_path'].replace("${env}", env).replace("${source}", metadata['source_name'])
    
    # Apply partitioning if specified
    if 'partitioning' in metadata and 'silver' in metadata['partitioning']:
        partition_info = metadata['partitioning']['silver'][0]  # Assuming we're using the first partition specification
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
        
        df.write \
            .format("delta") \
            .partitionBy(f"{partition_col}_partitioned") \
            .mode("append") \
            .save(silver_path)
    else:
        df.write \
            .format("delta") \
            .mode("append") \
            .save(silver_path)

def write_to_quarantine(df, metadata, env):
    quarantine_path = metadata['quarantine_path'].replace("${env}", env).replace("${source}", metadata['source_name'])
    
    df.write \
        .format("delta") \
        .mode("append") \
        .save(quarantine_path)

def process_data(df, metadata, env):
    enriched_df = enrich_data(df, metadata)
    valid_df, invalid_df = validate_data(enriched_df, metadata)
    print("invalid_df -->")
    invalid_df.show()
    
    silver_path = metadata['silver_path'].replace("${env}", env).replace("${source}", metadata['source_name'])
    
    try:
        # Write to silver layer
        write_to_silver(valid_df, metadata, env)
        
        # Verify the write was successful
        verify_silver_write(silver_path, valid_df.count())
        
        if invalid_df is not None and invalid_df.count() > 0:
            write_to_quarantine(invalid_df, metadata, env)
    except Exception as e:
        print(f"Error in processing data: {str(e)}")
        raise
    
    return valid_df, invalid_df

def verify_silver_write(silver_path, expected_count):
    max_retries = 5
    retry_interval = 10  # seconds
    
    for i in range(max_retries):
        try:
            actual_count = spark.read.format("delta").load(silver_path).count()
            if actual_count >= expected_count:
                print(f"Silver write verified. Expected: {expected_count}, Actual: {actual_count}")
                return
            else:
                print(f"Attempt {i+1}: Silver write not yet complete. Expected: {expected_count}, Actual: {actual_count}")
        except Exception as e:
            print(f"Attempt {i+1}: Error verifying silver write: {str(e)}")
        
        if i < max_retries - 1:
            print(f"Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    
    raise Exception(f"Failed to verify silver write after {max_retries} attempts")


def main(source=None, env=None):
    if source is None or env is None:
        print("Usage: bronze_to_silver --source <source> --env <environment>")
        sys.exit(1)
    
    metadata = get_metadata(source, env)
    print(f"Loaded metadata for source: {source}, environment: {env}")

    source_type = metadata['source']['type']
    
    if source_type == 'kafka':
        df = read_from_kafka(metadata)
    elif source_type == 'adls':
        df = read_from_bronze(metadata, env)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    # Now process the static DataFrame
    valid_df, invalid_df = process_data(df, metadata, env)
    print(f"Processing completed. Valid records: {valid_df.count()}, Invalid records: {invalid_df.count() if invalid_df else 0}")

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