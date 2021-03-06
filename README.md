# Spark Batch Process POC
## Summary
This is a simple demonstration of Batch processing in Spark using Scala language and hadoop HDFS file system. 

### Attribution
![](images/Page19.png)

### About Spark
![](images/Page1.png)
![](images/Page2.png)
![](images/Page3.png)
![](images/Page4.png)
![](images/Page5.png)

### Spark Cluster
All cloud providers will offer automated installation and configuration options
![](images/Pict1.png)

## Transaction Files and Master Data Files
### Staging the files
![](images/Pict2.png)
### Converting into Parquet 
| CSV TO PARQUET | [csvtoparquet.scala](source_code/csvtoparquet.scala) |
| -------------- | ---------------------------------------------------- |

![](images/Page6.png)
![](images/Page7.png)

## How to install SBT and use it
```
Sign into master node as hadoop user
From home directory run commends

sudo curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum install sbt
```

```
Directory Structure : /user/hadoop/sbt_workspace/perpinv/src/main/scala
```

Configuration File at (/user/hadoop/sbt_workspace/perpinv/build.sbt)
```
name := "spark perpinv App"
version := "0.1"
organization := "com.poc"
scalaVersion := "2.11.8"
val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % sparkVersion,
"org.apache.spark" %% "spark-sql" % sparkVersion)
```

Create source in /user/hadoop/sbt_workspace/perpinv/src/main/scala as :
![](images/Page8.png)

To package JAR files using SBT:
```
sbt package
```

To submit it to Spark
```
spark-submit --master yarn --class com.poc.examples.csvtoparquet target/scala-2.11/spark-perpinv-app_2.11-0.1.jar
```

Hadoop File System Screen Shot
![](images/Page9.png)

HDFS Name Node (Master)
![](images/HDFS_NAME_NODE.png)
HDFS Data Node (Slave)
![](images/HDFS_DATA_NODE.png)

YARN Resource Manager (Master)
![](images/YARN_RESOURCE_MANAGER.png)
YARN Node Manager (Slave)
![](images/YARN_NODE_MANAGER.png)

SPARK History Server Directed Acyclic Graph Example
![](images/SPARK_DAG.png)
SPARK Logical and Physical Plans
![](images/SPARK_PLANS.png)
Zepplin Notebook
![](images/ZEPPELIN_NOTEBOOK.png)


## Create Spark SQL Metadata for transaction files

| STG DDL | [stageddl.sql](source_code/stageddl.sql) |
| -------------- | --------------------------------- |

Spark Table Metadata
![](images/Page10.png)

Output from the Metadata
![](images/Page11.png)


## Create Transaction Header and Transaction Details files in CSV

|Process | Source Code |
| -------------- | -------------------------------------------------- |
| Process PMIX   | [processpmix.scala](source_code/processpmix.scala) |
| Process DELI   | [processdeli.scala](source_code/processdeli.scala) |
| Process INVADJ | [processinvadj.scala](source_code/processinvadj.scala) |
| Process STOCK | [processstock.scala](source_code/processstock.scala) |
| Process RESET | [processreset.scala](source_code/processreset.scala) |

## Create Spark SQL Metadata for transaction Header and Details files
| TRX DDL | [trxddl.sql](source_code/trxddl.sql) |
| -------------- | --------------------------------- |

Output from the Metadata
![](images/Page12.png)

## Create Spark SQL Metadata for results files
| STG STCK PPO DDL | [sspddl.sql](source_code/sspddl.sql) |
| -------------- | --------------------------------- |

Output from the Metadata
![](images/Page13.png)

## Run Final results file transformation

|Process | Source Code |
| -------------- | -------------------------------------------------- |
|Process STG STCK PPO   | [processstgstckppo.scala](source_code/processstgstckppo.scala) |

Functional Creation and Deployment
![](images/Page15.png)

Final Output
![](images/Page14.png)

