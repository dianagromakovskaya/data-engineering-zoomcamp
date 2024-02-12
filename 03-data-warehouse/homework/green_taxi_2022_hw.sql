-- Create an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-412710.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-de-course-412710-green-taxi-data/green/green_tripdata_2022-*.parquet']
);

-- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
CREATE OR REPLACE TABLE `dtc-de-course-412710.ny_taxi.green_tripdata_non_partitioned`
AS SELECT * FROM `dtc-de-course-412710.ny_taxi.external_green_tripdata`;

-- Question 1: What is count of records for the 2022 Green Taxi Data??
SELECT COUNT(*)
FROM `dtc-de-course-412710.ny_taxi.external_green_tripdata`;

-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT COUNT(DISTINCT PULocationID)
FROM `dtc-de-course-412710.ny_taxi.external_green_tripdata`;

SELECT COUNT(DISTINCT PULocationID)
FROM `ny_taxi.green_tripdata_non_partitioned`;

-- How many records have a fare_amount of 0?
SELECT COUNT(*)
FROM `dtc-de-course-412710.ny_taxi.green_tripdata_partitoned`
WHERE fare_amount = 0;

-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
CREATE OR REPLACE TABLE `dtc-de-course-412710.ny_taxi.green_tripdata_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID
   AS
SELECT * FROM `dtc-de-course-412710.ny_taxi.external_green_tripdata`;

-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
SELECT DISTINCT PULocationID
FROM `dtc-de-course-412710.ny_taxi.green_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT PULocationID
FROM `dtc-de-course-412710.ny_taxi.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
