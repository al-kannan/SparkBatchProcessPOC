package com.poc.examples
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object processbdaytx {
  def main(args: Array[String]) = {
    val spark  = SparkSession.builder()
			.appName("PerpInv")
			.enableHiveSupport()
			.getOrCreate()

    println("Processing BDAY TX ..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    val bday_tx_df = spark.sql(" select 'INVADJ' tx_type_cd, siteid, bday, is_no  from perpinv.invadj_stk3_bday_tx union select 'DELI' tx_type_cd, siteid, bday, is_no from perpinv.deli_stk3_bday_tx union select 'PMIX' tx_type_cd, siteid, bday, is_no from perpinv.pmix_stk3_bday_tx union select 'RESET' tx_type_cd, siteid, bday, is_no from perpinv.reset_stk3_bday_tx union select 'STOCK' tx_type_cd, siteid, bday, is_no from perpinv.stock_stk3_bday_tx")

    bday_tx_df.write
	.format("csv")
	.mode("overwrite")
	.save("hdfs:///user/root/PerpInv/stk3_bday_tx.csv")

    println("Processing end BDAY TX ..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    spark.stop()
  }
}

