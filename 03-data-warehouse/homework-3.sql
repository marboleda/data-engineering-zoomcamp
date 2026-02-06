-- Creating external table by referring to GCS path
CREATE OR REPLACE EXTERNAL TABLE dtc-de-course-484521.zoomcamp.external_yellow_trip_data
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-de-course-484521-terra-bucket/yellow_tripdata_2024-*.parquet']
);

-- Validate that we have data
SELECT * FROM dtc-de-course-484521.zoomcamp.external_yellow_trip_data LIMIT 10;

-- Create a non-partitioned table from the external table
CREATE OR REPLACE TABLE dtc-de-course-484521.zoomcamp.yellow_trip_data_nonpartitioned AS
SELECT * FROM dtc-de-course-484521.zoomcamp.external_yellow_trip_data;

-- Homework Question #1: What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(*) FROM dtc-de-course-484521.zoomcamp.yellow_trip_data_nonpartitioned;

/*
 * Homework Question #2:
 * Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
 * 
 * What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
 */
SELECT COUNT(DISTINCT PULocationID)
FROM dtc-de-course-484521.zoomcamp.external_yellow_trip_data;

SELECT COUNT(DISTINCT PULocationID)
FROM dtc-de-course-484521.zoomcamp.yellow_trip_data_nonpartitioned;

/*
 * Homework Question #3:
 * Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.
 * 
 * Why are the estimated number of Bytes different?
 */
SELECT PULocationID
FROM dtc-de-course-484521.zoomcamp.yellow_trip_data_nonpartitioned;

SELECT PULocationID, DOLocationID
FROM dtc-de-course-484521.zoomcamp.yellow_trip_data_nonpartitioned;

-- Homework Question #4: How many records have a fare_amount of 0?
SELECT COUNT(*)
FROM dtc-de-course-484521.zoomcamp.yellow_trip_data_nonpartitioned
WHERE fare_amount = 0;

-- Homework Question #5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

-- Create a new table partitioned by tpep_dropoff_datetime and clustered by VendorID
CREATE OR REPLACE TABLE dtc-de-course-484521.zoomcamp.yellow_trip_data_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID 
  AS
SELECT * FROM dtc-de-course-484521.zoomcamp.external_yellow_trip_data;

-- Look into partitions
SELECT table_name, partition_id, total_rows
FROM `dtc-de-course-484521.zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_trip_data_partitioned_clustered'
ORDER BY total_rows DESC;

/*
 * Homework Question #6:
 * Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
 *
 * Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
 *
 */
SELECT DISTINCT(VendorID)
FROM dtc-de-course-484521.zoomcamp.yellow_trip_data_nonpartitioned
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15';

SELECT DISTINCT(VendorID)
FROM dtc-de-course-484521.zoomcamp.yellow_trip_data_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15';

-- Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
SELECT COUNT(*) FROM dtc-de-course-484521.zoomcamp.yellow_trip_data_nonpartitioned;
