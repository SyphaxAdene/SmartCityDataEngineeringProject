from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, udf, struct, row_number, array_sort, collect_list, acos, sin, radians, cos
from pyspark.sql.window import Window
import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import timedelta
from pyspark.dbutils import DBUtils
import argparse
from pyspark.sql.types import StringType, DoubleType, MapType


# Global variables
spark = None
dbutils = None

def get_api_credentials():
    username = dbutils.secrets.get(scope="meteomatics-scope", key="api_username")
    password = dbutils.secrets.get(scope="meteomatics-scope", key="api_password")
    return username, password

def construct_api_url(start_date, end_date, parameters, location):
    time_interval = f"{start_date.isoformat()}Z--{end_date.isoformat()}Z:PT1H"
    return f'https://api.meteomatics.com/{time_interval}/{",".join(parameters)}/{location}/json'

def fetch_weather_data(url, username, password):
    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
        print(f'Response content: {response.text}')
    except requests.exceptions.RequestException as req_err:
        print(f'An error occurred while making the request: {req_err}')
    except json.JSONDecodeError as json_err:
        print(f'Error decoding JSON: {json_err}')
        print(f'Response content: {response.text}')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
    return None

def get_weather_for_event(start_time, lat, lon, username, password):
    parameters = [
        't_2m:C',
        'relative_humidity_2m:p',
        'wind_speed_10m:ms',
        'precip_1h:mm',
        'total_cloud_cover_mean_1h:p',
        'is_rain_1h:idx',
        'is_snow_1h:idx',
        'uv:idx'
    ]
    url = construct_api_url(start_time, start_time + timedelta(hours=1), parameters, f"{lat},{lon}")
    data = fetch_weather_data(url, username, password)
    if data and 'data' in data:
        return json.dumps({param['parameter']: param['coordinates'][0]['dates'][0]['value'] for param in data['data']})
    return "{}"

def enrich_events_with_weather(events_df, username, password):
    weather_udf = udf(lambda start_time, lat, lon: get_weather_for_event(start_time, lat, lon, username, password), StringType())
    return events_df.withColumn("weather_data", weather_udf(col("startTime"), col("latitude"), col("longitude")))

def store_as_delta_table(df, path, mode="overwrite"):
    df = df.withColumn("processedTimestamp", current_timestamp())
    df.write.format("delta").mode(mode).save(path)
    print(f"Data successfully written to {path}")

def read_parking_data_from_gold(env):
    gold_path = f"abfss://data@syphaxstoragedev.dfs.core.windows.net/{env}/gold/parking_data/"
    return spark.read.format("delta").load(gold_path)

def read_events_from_gold(env):
    gold_path = f"abfss://data@syphaxstoragedev.dfs.core.windows.net/{env}/gold/events_data/"
    return spark.read.format("delta").load(gold_path)

def calculate_distance_km(lat1, lon1, lat2, lon2):
    return (
        acos(
            sin(radians(lat1)) * sin(radians(lat2)) +
            cos(radians(lat1)) * cos(radians(lat2)) *
            cos(radians(lon2) - radians(lon1))
        ) * 6371
    )

def main(env="dev"):
    # Read parking data from gold layer
    parking_df = read_parking_data_from_gold(env)

    # Read events data from gold layer
    events_df = read_events_from_gold(env)

    # Get API credentials
    username, password = get_api_credentials()

    # Enrich events with weather data
    enriched_events_df = enrich_events_with_weather(events_df, username, password)

    # Define the schema for the weather data
    weather_schema = MapType(StringType(), DoubleType())

    # Parse the weather_data JSON string into a map
    enriched_events_df = enriched_events_df.withColumn("weather_data", from_json(col("weather_data"), weather_schema))

    # Explode the weather_data column into separate columns
    for param in ['t_2m:C', 'relative_humidity_2m:p', 'wind_speed_10m:ms', 'precip_1h:mm', 
                  'total_cloud_cover_mean_1h:p', 'is_rain_1h:idx', 'is_snow_1h:idx', 'uv:idx']:
        enriched_events_df = enriched_events_df.withColumn(param.split(':')[0], col("weather_data").getItem(param))

    # Drop the original weather_data column
    enriched_events_df = enriched_events_df.drop("weather_data")

    # Calculate distances and create an array of nearby events for each parking station
    joined_df = parking_df.crossJoin(enriched_events_df)
    
    joined_df = joined_df.withColumn(
        "distance_km", 
        calculate_distance_km(
            parking_df.latitude, 
            parking_df.longitude, 
            enriched_events_df.latitude, 
            enriched_events_df.longitude
        )
    )

    # Filter events within 5 km and order by distance
    window_spec = Window.partitionBy("stationId").orderBy("distance_km")
    nearby_events_df = joined_df.filter(col("distance_km") <= 5).withColumn(
        "row_num", row_number().over(window_spec)
    ).filter(col("row_num") <= 5)  # Limit to top 5 nearest events

    # Create a struct for each nearby event
    event_struct = struct(
        col("eventId"), 
        col("eventName"), 
        col("eventType"), 
        col("startTime"), 
        col("endTime"), 
        col("distance_km"),
        col("t_2m"),
        col("relative_humidity_2m"),
        col("wind_speed_10m"),
        col("precip_1h"),
        col("is_rain_1h"),
        col("is_snow_1h"),
        col("uv")
    )

    # Aggregate events for each parking station
    final_df = nearby_events_df.groupBy("stationId").agg(
        array_sort(collect_list(event_struct)).alias("nearby_events")
    )

    # Join back with the original parking data to include all parking information
    final_df = parking_df.join(final_df, "stationId", "left_outer")

    # Store the final DataFrame as a Delta table in the smart_city_parking folder
    smart_city_path = f"abfss://data@syphaxstoragedev.dfs.core.windows.net/{env}/gold/smart_city_parking/"
    store_as_delta_table(final_df, smart_city_path)

    return final_df

def cli_main():
    global spark, dbutils
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, help="Environment (dev, prod, etc.)")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("SmartCityParkingAnalysis").getOrCreate()
    dbutils = DBUtils(spark)
    # Set the storage account key in Spark configuration
    spark.conf.set(
        "fs.azure.account.key.syphaxstoragedev.dfs.core.windows.net",
        dbutils.secrets.get(scope="adls-scope", key="adls-key")
    )
    
    final_df = main(args.env)
    print("The final df -->")
    final_df.show(truncate=False)

if __name__ == "__main__":
    cli_main()