from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType

from pyspark.sql.functions import col, unix_timestamp, round, rank
# TODO: Import any other pyspark.sql.functions you might need (e.g., count, desc, window)
from pyspark.sql.window import Window
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

def get_private_key_string(key_path, password=None):
    """Reads a PEM private key and returns the string format required by PySpark."""
    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password.encode() if password else None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Spark requires the raw key string without headers, footers, or newlines
    pkb_str = pkb.decode("utf-8")
    pkb_str = pkb_str.replace("-----BEGIN PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("-----END PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("\n", "")
    return pkb_str

def create_spark_session():
    """
    Initializes and returns a SparkSession connected to the LinuxLab cluster.
    """
    try:
        # Dynamically map to the active LinuxLab node and port
        node = os.environ['SLURMD_NODENAME']
        master_port = os.environ['SPARK_MASTER_PORT']
        master_url = f"spark://{node}.engr.wustl.edu:{master_port}"
    except KeyError:
        print("Warning: LinuxLab environment variables not found.")
        print("Falling back to local mode for local testing...")
        master_url = "local[*]"

    spark = SparkSession.builder \
        .appName("NYCTaxiAnalytics_2023") \
        .master(master_url) \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.13:2.12.0-spark_3.4") \
        .getOrCreate()
    return spark

def clean_taxi_data(df):
    """
    Part 1: Clean the raw trip data.
    """
    df = df.filter(
        (df.passenger_count.isNotNull()) & (df.passenger_count != 0) &
        (df.trip_distance > 0) & 
        (df.fare_amount > 0)
    )
    df = df.withColumn("trip_duration_minutes", 
        ((unix_timestamp(df.tpep_dropoff_datetime) - unix_timestamp(df.tpep_pickup_datetime)) / 60)
        .cast('long')
    )
    df = df.filter(
        (df.trip_duration_minutes >= 1) & (df.trip_duration_minutes <= 120)
    )
    return df

def join_zone_lookups(trip_df, zone_df):
    """
    Part 2.1: Join trip data with zone lookups.
    """
    # Exluding zone_df.service_zone column from final result
    zone_df = zone_df.select(zone_df.LocationID, zone_df.Borough)

    # PULocation join
    joined_df = trip_df.join(
        zone_df.withColumnRenamed("Borough", "PU_Borough"),
        trip_df.PULocationID == zone_df.LocationID, 
        "left"
    ).drop("LocationID")

    # DOLocation join
    joined_df = joined_df.join(
        zone_df.withColumnRenamed("Borough", "DO_Borough"), 
        trip_df.DOLocationID == zone_df.LocationID, 
        "left"
    ).drop("LocationID")

    return joined_df

def calculate_busiest_boroughs(joined_df):
    """
    Part 2.2: Calculate total trips per pickup borough.
    """
    joined_df = joined_df.groupBy(col("PU_Borough")).count() \
        .orderBy(col("count").desc())
    return joined_df

def calculate_top_dropoff_zones_by_pickup(joined_df):
    """
    Part 3: Advanced Analytics using Window Functions.
    """
    # Manhattan, Manhattan, 123 --> rank 1
    # Manhattan, Brooklyn, 54 --> rank 2
    # Manhattan, NJ, 27 --> rank 3

    count_by_boroughs = joined_df.groupBy(col("PU_Borough"), col("DO_Borough")).count()
    window = Window.partitionBy("PU_Borough").orderBy(col("count").desc())
    ranked_counts = count_by_boroughs.withColumn("rank", rank().over(window))
    top_3_counts = ranked_counts.filter(col("rank") <= 3).orderBy("PU_Borough", "rank").drop("rank")

    return top_3_counts

def write_to_snowflake(df, table_name):
    """
    Part 5: Data Warehousing with Snowflake using Key Pair Auth.
    """
    # TODO: Load your private key into a variable using the helper function
    pkb_string = get_private_key_string("/home/linus-dannull/Documents/WashUCoding/Data Engineering/assignment4_spark/spark_starter_code/rsa_key.p8")

    sfOptions = {
      "sfURL": "sfedu02-unb02139.snowflakecomputing.com",
      "sfUser": "FERRET",
      "sfDatabase": "FERRET_DB",
      "sfSchema": "PUBLIC",
      "sfWarehouse": "FERRET_WH",
      "pem_private_key": pkb_string
    }

    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()

def extra_credit(joined_df):
    """
    Part 6: Extra Credit
    Implement your extra credit logic here. Leave comments explaining what you built!
    """
    # TODO: (Optional) Your awesome code here
    pass

def main():
    spark = create_spark_session()

    # Load Data
    # Notice we added mergeSchema for Parquet, and inferSchema for CSV to prevent crashes!
    trip_path = "data/yellow_tripdata_2023-*.parquet"
    zone_path = "data/taxi_zone_lookup.csv"

    print("Loading data into DataFrames...")
    # TODO: fix this code to handle schema drift and potential loading issues gracefully
    schema = StructType([
        StructField("VendorID", DoubleType(), True),  # Cast to DOUBLE
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),  # Cast to DOUBLE
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), True),  # Cast to DOUBLE
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", DoubleType(), True),  # Cast to DOUBLE
        StructField("DOLocationID", DoubleType(), True),  # Cast to DOUBLE
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True)  # Cast to DOUBLE
    ])
    try:
        raw_trips = spark.createDataFrame([], schema=schema)

        for filePath in os.listdir("data"):
            if ".csv" in filePath:
                continue
            trip_path = os.path.join("data", filePath)
            print("reading", trip_path)
            df_raw = spark.read.parquet(trip_path)
            df_raw = df_raw.withColumn(
                "passenger_count", 
                col("passenger_count").cast("long")
            )
            raw_trips = raw_trips.unionByName(df_raw, allowMissingColumns=True)
        
        zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_path)
    except Exception as e:
        print(f"Error loading data. Did you run download_data.py? Error: {e}")
        spark.stop()
        exit(1)

    # print("Showing raw_trips data")
    raw_trips.show(10)
    raw_trips.printSchema()

    # print("Showing zones data")
    # zones.show(10)

    print("Cleaning data...")
    cleaned_trips = clean_taxi_data(raw_trips)

    print("Joining zone lookups...")
    joined_data = join_zone_lookups(cleaned_trips, zones)
    # joined_data.show(10)

    print("Caching joined data in memory...")
    joined_data.cache()

    # Materialize Cache
    print(f"Total trips after cleaning: {joined_data.count():,}")

    print("\n--- Busiest Pickup Boroughs ---")
    busiest_boroughs = calculate_busiest_boroughs(joined_data)
    busiest_boroughs.show()

    print("\n--- Top 3 Dropoff Zones per Pickup Borough ---")
    top_dropoffs = calculate_top_dropoff_zones_by_pickup(joined_data)
    top_dropoffs.show(15, truncate=False)

    busiest_boroughs.printSchema()

    # Part 5: Write to Snowflake
    dst_table_name = "TAXI_BUSIEST_BOROUGHS"
    print("Writing data to", dst_table_name)
    write_to_snowflake(busiest_boroughs, dst_table_name)

    # Run extra credit
    # extra_credit(joined_data)

    spark.stop()

if __name__ == "__main__":
    main()