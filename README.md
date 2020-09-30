# db2datalake

Simple database scrapper with Apache-SPARK

Goals :
- Rationalize frameworks used inside the cluster
- Avoid wrong behaviour from apache-sqoop (avro usage in background)
- Control easily the parallelism of the extraction.
- Register easily a bunch of parquet files in hive.
- Correct the 32k characters bug of SAS (SAS download the full size of a String type)

## HiveRegister
Create the command for registering a file in hive. By the same time register String as VarChar(size).

### HiveRegister - execution parameters
- **input_folder** : hdfs directory containing the file to register
- **tables_list** : list of tables to register
- **hive_db** : the hive db where the tables will registered
- **parallelism_table** : the number of tables to analyze in parallel
- **parallelism_col** : the number of columns to check in parallel per table.

### HiveRegister - recommendation
- use HiveRegisterLauncher as main class for the spark-submit
- the total number of columns analyzed in parallel should never be superior to the total amount of cores. `<parallelism_table> * <parallelism_cols> <= <spark.executor.instances> * <spark.executor.cores> `

## ScrapperSQL
Download database tables through jdbc.It partitions data to accelerate the retrieving process.
### ScrapperSQL - execution parameters
- **connection_string** : `"jdbc:<subprotocol>://<hostname>/<schema>;<param1>;<param2>"`
- **jdbc_driver** : java driver name (ex "com.ibm.db2.jcc.DB2Driver")
- **tables_list** : `<table1>,<table2>,<table3>,...` (comma separated and no spaces)
- **db_user** : user used to access the  database
- **jceks_path** : path to the file containing the encrypted password for the user.
- **output_path** : path on HDFS to drop off the extracted data
- **parallelism_tables** : number of tables to extract in parallel.
- **connexion_per_table** : number of partition per table (the number of partition working in parallel depends on the number of cores available).

### ScrapperSQL - recommendation
- parallelism_tables : the value should never be superior to the total amount of cores used for the job (between 1/3 and 1/2 the number of cores).
- connection_string : semi-colon must be escaped when the program is tested through ssh ( \\; )
- connexion_per_table : this value should be fine tuned :
- - Too much will cause error int the extraction (when there are more partitions than distinct values).
  - Too few will throw OutOfMemory on huge table.
- add new jdbc driver :
  1. Deploy a new version with the driver packaged with it
  2. Deploy the jar on edge node and add `--jars file:/path_to_jar` to the spark-submit command

## ScrapperMongo
Download MongoDB collections through the specific connector spark-mongo.It partitions data to accelerate the retrieving process
### ScrapperMongo - execution parameters
- **mongo_servers** : `"<broker1_url>:<port>,<broker2_url>:<port>, ..."`
- **collections_list** : `<collection1>,<collection2>,<collection3>,...` (comma separated and no spaces)
- **db_user** : user used to access the  database
- **jceks_path** : path to the file containing the encrypted password for the user.
- **output_path** : path on HDFS to drop off the extracted data
- **parallelism_collection** : number of collection to extract in parallel.

### ScrapperMongo - recommendation
- When infering data type , sometimes spark return nulltype which is incompatible with parquet file
- - NullType are replaced by StringType by default.
