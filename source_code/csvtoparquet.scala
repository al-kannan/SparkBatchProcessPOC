package com.poc.examples

    import org.apache.spark.sql._
    import org.apache.spark.sql.types._

    object csvtoparquet {
        def main(args: Array[String]) = {
            val spark  = SparkSession.builder()
                                     .appName("PerpInv")
                                     .enableHiveSupport()
                                     .getOrCreate()

	    val stockSchema = StructType(Array(StructField("is_no",LongType,true), 
				StructField("siteid",LongType,true), 
				StructField("su",StringType,true), 
				StructField("transferday",DateType,true), 
				StructField("trx_date",DateType,true), 
				StructField("stock_qty_units",DoubleType,true)) 
                                )


	    val pmixSchema = StructType(Array(StructField("is_no",LongType,true), 
				StructField("siteid",LongType,true), 
				StructField("menuitem",StringType,true), 
				StructField("transferday",DateType,true), 
				StructField("trx_date",DateType,true), 
				StructField("sales_qty_units",DoubleType,true)) 
                                )


	   val deliSchema = StructType(Array(StructField("is_no",LongType,true), 
				StructField("siteid",LongType,true), 
				StructField("su",StringType,true), 
				StructField("transferday",DateType,true), 
				StructField("trx_date",TimestampType,true), 
				StructField("delivery_qty_cases",DoubleType,true)) 
                                )


	   val invadjSchema = StructType(Array(StructField("is_no",LongType,true), 
				StructField("siteid",LongType,true), 
				StructField("su",StringType,true), 
				StructField("transferday",DateType,true), 
				StructField("trx_date",DateType,true), 
				StructField("adj_qty_cases",DoubleType,true)) 
                                )


	   val resetSchema = StructType(Array(StructField("is_no",LongType,true), 
				StructField("siteid",LongType,true), 
				StructField("pponumber",StringType,true), 
				StructField("transferday",DateType,true), 
				StructField("trx_date",DateType,true), 
				StructField("reset_qty_units",DoubleType,true)) 
                                )


	   val bmitoppoSchema = StructType(Array(
				StructField("siteid",LongType,true), 
				StructField("menuitem",StringType,true), 
				StructField("pponumber",StringType,true), 
				StructField("usage_qty",DoubleType,true)) 
                                )

	   val ppotosuSchema = StructType(Array(
				StructField("siteid",LongType,true), 
				StructField("pponumber",StringType,true), 
				StructField("su",StringType,true), 
				StructField("unitpp",DoubleType,true)) 
                                )


             val stock_df = spark.read
                    .format("csv")
                    .schema(stockSchema)
                    .option("header","true")
                    .option("nullValue","NA")
                    .option("dateFormat", "MM/dd/yyyy")
                    .option("mode","failfast")
                    .load("hdfs:///user/root/stg_stock.csv")

	      println("Stock Schema:")

              stock_df.printSchema()

              stock_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save("hdfs:///user/root/PerpInv/stg_stock.parquet")

             val pmix_df = spark.read
                    .format("csv")
                    .schema(pmixSchema)
                    .option("header","true")
                    .option("nullValue","NA")
                    .option("dateFormat", "MM/dd/yyyy")
                    .option("mode","failfast")
                    .load("hdfs:///user/root/stg_pmix.csv")

	      println("PMIX Schema:")

              pmix_df.printSchema()

	      //pmix_df.select("is_no", "siteid").filter("is_no = 1069250 and siteid = 11884").show 

              pmix_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save("hdfs:///user/root/PerpInv/stg_pmix.parquet")

             val deli_df = spark.read
                    .format("csv")
                    .schema(deliSchema)
                    .option("header","true")
                    .option("nullValue","NA")
                    .option("dateFormat", "MM/dd/yyyy")
                    .option("timestampFormat", "MM/dd/yyyy HH:mm:ss")
                    .option("mode","failfast")
                    .load("hdfs:///user/root/stg_deli.csv")

	      println("DELI Schema:")

              deli_df.printSchema()

              deli_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save("hdfs:///user/root/PerpInv/stg_deli.parquet")

               val invadj_df = spark.read
                    .format("csv")
                    .schema(invadjSchema)
                    .option("header","true")
                    .option("nullValue","NA")
                    .option("dateFormat", "MM/dd/yyyy")
                    .option("mode","failfast")
                    .load("hdfs:///user/root/stg_invadj.csv")

	      println("InvADJ Schema:")

              invadj_df.printSchema()

              invadj_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save("hdfs:///user/root/PerpInv/stg_invadj.parquet")

             val reset_df = spark.read
                    .format("csv")
                    .schema(resetSchema)
                    .option("header","true")
                    .option("nullValue","NA")
                    .option("dateFormat", "MM/dd/yyyy")
                    .option("mode","failfast")
                    .load("hdfs:///user/root/stg_reset.csv")

	      println("RESET Schema:")

              reset_df.printSchema()

              reset_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save("hdfs:///user/root/PerpInv/stg_reset.parquet")

             val bmitoppo_df = spark.read
                    .format("csv")
                    .schema(bmitoppoSchema)
                    .option("header","true")
                    .option("nullValue","NA")
                    .option("mode","failfast")
                    .load("hdfs:///user/root/bmi_to_ppo.csv")

	      println("BMITOPPO Schema:")

              bmitoppo_df.printSchema()

              bmitoppo_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save("hdfs:///user/root/PerpInv/stg_bmitoppo.parquet")

             val ppotosu_df = spark.read
                    .format("csv")
                    .schema(ppotosuSchema)
                    .option("header","true")
                    .option("nullValue","NA")
                    .option("mode","failfast")
                    .load("hdfs:///user/root/ppo_to_su.csv")

	      println("PPOTOSU Schema:")

              ppotosu_df.printSchema()

              ppotosu_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save("hdfs:///user/root/PerpInv/stg_ppotosu.parquet")

            spark.stop()
        }
    }

