1. CREATE OR REPLACE EXTERNAL TABLE `alexy-de-bootcamp.nytaxi_dataset.yellow_tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://alexy-de-bootcamp-terra-bucket/yellow_tripdata_2024-*.parquet']
);  -- that doesn't create a physical table in bigquery instead it reads the data directly from GCS

CREATE OR REPLACE TABLE `alexy-de-bootcamp.nytaxi_dataset.yellow_tripdata`
AS
SELECT * FROM `alexy-de-bootcamp.nytaxi_dataset.yellow_tripdata_external`;  -- that will create a physical table in bigquery


2.   select  count(distinct PULocationID) 
  from `nytaxi_dataset.yellow_tripdata_external`

    select  count(distinct PULocationID) 
  from `nytaxi_dataset.yellow_tripdata`


4. select count(*) from `nytaxi_dataset.yellow_tripdata`
where fare_amount = 0;

5. 
create or replace table `nytaxi_dataset.yellow_tripdata_partitioned_clustered`
PARTITION by date(tpep_dropoff_datetime)
cluster by vendorid AS
select * from `nytaxi_dataset.yellow_tripdata`;

6. select distinct vendorid
from nytaxi_dataset.yellow_tripdata_partitioned_clustered
where tpep_dropoff_datetime >= '2024-03-01' and  tpep_dropoff_datetime <= '2024-03-15';

