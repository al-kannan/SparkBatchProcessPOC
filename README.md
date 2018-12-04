# Spark Batch Process POC
## Summary
This is a simple demonstration of Batch processing in Spark using Scala language and hadoop HDFS file system. 

### About Spark
![](images/Page1.png)
![](images/Page2.png)
![](images/Page3.png)
![](images/Page4.png)
![](images/Page5.png)

### Standalone Spark Install
<screenshot>

### Spark Cluster
<screenshot>

### Lessons Learned
<screenshot>
	- What is Spark
		- Distributed Processing Engine
		- Memory based processing model
		- Our code is abstracted from distributed processing
		- Lazy processing model
	- What is not Spark
		- It is not a database, no transactions
		- It is not for storage systems
		- Not good for too many sessions
	- Spark Architecture
		- Driver and Executor
		- Jar file is deployed to Executor to run it on subset of data
		- Shuffle is done by the engine
		- RDD is the code component to hold data and instruction details
		- Task and Stages and managed by Spark engine	
			(Directed Acyclic Graph)
	- Spark Resource Managers (Yarn and Mesos)
	- Spark Connecters for data sources
	- Spark does not need HDFS all though use HIVE for metastore
	- Spark can be installed on your laptop as standalone
	- Spark has intractive mode and batch mode
		- Intractive mode include 
			Spark-Shell (Scala)
			PySpark (Python) 
			Spark-SQL
			Note books (Jupyter and Zeplin)
		- spark-submit using Yarn

## Transaction Files and Master Data Files
### Staging the files
### Converting into Parquet 
<code insert>
### Lessons Learned
	- Spark supports many different file formats
	- Spark supports four different API methods
		-- RDD APIs
		-- DF APIs
		-- Data Sets APIs
		-- Spark-SQL table definition
	- How to install SBT and use it

## Create Spark SQL Metadata for transaction files
Lessons Learned
	- Metadata can be created and managed in local file system
		or it can be managed using Hive metastore
	- You can also implement temporary views within a spark context
	
## Create Transaction Header and Transaction Details files in CSV

## Create Spark SQL Metadata for transaction Header and Details files

## Bring Stg Stck PPO (End Results) file for merging

## Create Spark SQL Metadata for results files

## Run Final results file transformation

