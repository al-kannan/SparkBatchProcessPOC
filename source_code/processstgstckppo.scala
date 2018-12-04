package com.poc.examples

    import org.apache.spark.sql._
    import org.apache.spark.sql.types._

    object processstgstckppo {
        def main(args: Array[String]) = {
            val spark  = SparkSession.builder()
                                     .appName("PerpInv")
                                     .enableHiveSupport()
                                     .getOrCreate()

	    val stgstckppoSchema = StructType(Array(
				StructField("siteid",LongType,true), 
				StructField("bday",DateType,true), 
				StructField("pponumber",StringType,true), 
				StructField("stockqty",DoubleType,true),
				StructField("deliqty",DoubleType,true),
				StructField("invadjqty",DoubleType,true),
				StructField("pmixqty",DoubleType,true),
				StructField("onhand",DoubleType,true)
                                ) 
                                )

             val stgstckppo_df = spark.read
                    .format("csv")
		    .schema(stgstckppoSchema)
                    .option("header","false")
		    .option("nullValue","null")
                    .option("dateFormat", "MM/dd/yyyy")
                    .option("mode","permissive")
                    .load("hdfs:///user/root/stg_stck_ppo.csv")

	      println("Stg Stck PPO Schema:")

              stgstckppo_df.printSchema()

              stgstckppo_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save("hdfs:///user/root/PerpInv/stg_stck_ppo.parquet")

	    // Transactions Transforamtion
	    val stgstckppo_tx_applied_df = spark.sql(" with tx_record as (select bs.siteid, bs.bday, tx.pponumber, max(if(tx.tx_type_cd == \"PMIX\", tx.qty, 0)) pmixqty, max(if(tx.tx_type_cd == \"STOCK\", tx.qty, 0)) stockqty, max(if(tx.tx_type_cd == \"RESET\", tx.qty, 0)) resetqty, max(if(tx.tx_type_cd == \"DELI\", tx.qty, 0)) deliqty, max(if(tx.tx_type_cd == \"INVADJ\", tx.qty, 0)) invadjqty  from perpinv.stk3_bday_status bs, perpinv.stk3_tx tx where bs.siteid = 11884 and tx.siteid = bs.siteid and tx.bday = bs.bday and tx.is_no in (bs.pmix_is_no, bs.stock_is_no, bs.deli_is_no, bs.invadj_is_no, bs.reset_is_no) group by bs.siteid, bs.bday, tx.pponumber )     select ssp.siteid, ssp.bday, ssp.pponumber, ifnull(txr.pmixqty, ifnull(ssp.pmixqty,0)) pmixqty, ifnull(txr.stockqty, ifnull(ssp.stockqty,0)) stockqty , ifnull(txr.deliqty, ifnull(ssp.deliqty,0)) deliqty , ifnull(txr.invadjqty, ifnull(ssp.invadjqty,0)) invadjqty, ifnull(txr.resetqty, ifnull(txr.stockqty, ifnull(ssp.stockqty,0))) resetqty  from perpinv.stg_stck_ppo ssp left outer join tx_record txr on ssp.siteid = txr.siteid and ssp.bday = txr.bday and ssp.pponumber = txr.pponumber and ssp.pponumber = \"600000790\" ")

	    stgstckppo_tx_applied_df.createTempView("stgstckppo_tx_applied")

	    stgstckppo_tx_applied_df.show(10);


	    val calculate_perp_inv_f = (pmixqty:Double, stockqty:Double, deliqty:Double, invadjqty:Double) => stockqty + deliqty + invadjqty - pmixqty

	    spark.udf.register("calculate_perp_inv", calculate_perp_inv_f)

	    spark.sql("select siteid, pponumber, bday, pmixqty, stockqty, deliqty, invadjqty , sum(calculate_perp_inv(pmixqty, stockqty, deliqty, invadjqty)) over (partition by siteid, pponumber order by bday) newonhand from stgstckppo_tx_applied where pponumber = \"600000790\" ").show()

            spark.stop()
        }
    }

