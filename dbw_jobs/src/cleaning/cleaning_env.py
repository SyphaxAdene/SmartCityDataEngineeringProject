from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import argparse

# Global variables
spark = None
dbutils = None

PROTECTED_ENVIRONMENTS = ['dev', 'qa', 'test', 'prod']

def get_base_path():
    return "abfss://data@syphaxstoragedev.dfs.core.windows.net"

def is_protected_path(path):
    base_path = get_base_path()
    for env in PROTECTED_ENVIRONMENTS:
        if path.startswith(f"{base_path}/{env}/"):
            return True
    return False

def clean_adls_folder(path):
    if is_protected_path(path):
        print(f"WARNING: Cannot clean protected folder: {path}")
        return

    print(f"Cleaning folder: {path}")
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"Folder {path} cleaned and removed successfully.")
    except Exception as e:
        print(f"Error while cleaning folder {path}: {str(e)}")

def clean_environment(env):
    if env.lower() in PROTECTED_ENVIRONMENTS:
        print(f"ERROR: Cannot clean protected environment: {env}")
        return

    base_path = get_base_path()
    env_path = f"{base_path}/{env}"
    
    print(f"Cleaning environment folder: {env_path}")
    
    try:
        # List contents of the folder
        contents = dbutils.fs.ls(env_path)
        
        # Clean all subfolders and files
        for item in contents:
            clean_adls_folder(item.path)
        
        # Remove the environment folder itself
        dbutils.fs.rm(env_path)
        print(f"Environment folder {env_path} removed successfully.")
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            print(f"Environment folder {env_path} does not exist.")
        else:
            print(f"Error while cleaning environment {env}: {str(e)}")

def main(env):
    global spark, dbutils
    spark = SparkSession.builder.appName("ADLSEnvironmentCleaner").getOrCreate()
    dbutils = DBUtils(spark)

    # Set the storage account key in Spark configuration
    spark.conf.set(
        "fs.azure.account.key.syphaxstoragedev.dfs.core.windows.net",
        dbutils.secrets.get(scope="adls-scope", key="adls-key")
    )

    clean_environment(env)

    print(f"Cleaning process completed for environment: {env}")

def cli_main():
    parser = argparse.ArgumentParser(description="Clean ADLS environment folder (except protected environments)")
    parser.add_argument("--env", required=True, help="Environment to clean (e.g., staging, temp)")
    args = parser.parse_args()
    main(args.env)

if __name__ == "__main__":
    cli_main()