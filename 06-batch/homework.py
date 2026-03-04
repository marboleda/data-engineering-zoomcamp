from pyspark.sql import SparkSession, functions as F, types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# Question 1: Version of Spark?
print(spark.version)

# Question 2
# Read the November 2025 Yellow into a Spark Dataframe.
# Repartition the Dataframe to 4 partitions and save it to parquet.
# What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.
df_yellow = spark.read.parquet('yellow_tripdata_2025-11.parquet')
df_yellow_repartitioned = df_yellow.repartition(4)
df_yellow_repartitioned.write.parquet('yellow_tripdata_2025-11_repartitioned', mode='overwrite')

df_yellow.registerTempTable('yellow_2025_11_trips')

df_yellow.printSchema()


# Question 3: How many taxi trips were there on the 15h of November?
# Consider only trips that started on the 15th of November
df_count = spark.sql("""
SELECT COUNT(*) AS trip_count
FROM yellow_2025_11_trips
WHERE tpep_pickup_datetime >= '2025-11-15 00:00:00' AND tpep_pickup_datetime < '2025-11-16 00:00:00'
""")
df_count.show()

# Question 4: What is the length of the longest trip in the dataset in hours?

# df_yellow = df_yellow.withColumn('trip_duration_hours', (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 3600)
df_yellow = df_yellow.withColumn('trip_duration_hours', F.timestamp_diff('HOUR', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'))
df_yellow.registerTempTable('yellow_2025_11_trips')

df_longest_trip = spark.sql("""
SELECT MAX(trip_duration_hours) AS longest_trip_duration
FROM yellow_2025_11_trips
""")
df_longest_trip.show()

# Question 6: Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?
zones_schema = types.StructType([
    types.StructField('LocationID', types.IntegerType(), False),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True)
])


df_zones = spark.read \
    .schema(zones_schema) \
    .csv('taxi_zone_lookup.csv', header=True)
df_zones.registerTempTable('zones')

df_pickup_zone_count = spark.sql("""
SELECT z.Zone, COUNT(*) AS pickup_count
FROM yellow_2025_11_trips y
     JOIN zones z ON y.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY pickup_count ASC
""")
df_pickup_zone_count.show()

spark.stop()