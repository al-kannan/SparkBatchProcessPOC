package com.poc.examples
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object processtx {
  def main(args: Array[String]) = {
    val spark  = SparkSession.builder()
			.appName("PerpInv")
			.enableHiveSupport()
			.getOrCreate()

    println("Processing TX ..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    val tx_df = spark.sql("select is_no, siteid, bday, pponumber, qty, 'INVADJ' tx_type_cd from perpinv.invadj_stk3_tx union select is_no, siteid, bday, pponumber, qty, 'DELI' tx_type_cd from perpinv.deli_stk3_tx union select is_no, siteid, bday, pponumber, qty, 'PMIX' tx_type_cd from perpinv.pmix_stk3_tx union select is_no, siteid, bday, pponumber, qty, 'RESET' tx_type_cd from perpinv.reset_stk3_tx union select is_no, siteid, bday, pponumber, qty, 'STOCK' tx_type_cd from perpinv.stock_stk3_tx")

    tx_df.write
	.format("csv")
	.mode("overwrite")
	.save("hdfs:///user/root/PerpInv/stk3_tx.csv")

    println("Processing end TX ..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    spark.stop()
  }
}


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

object processbdaystatus {
  def main(args: Array[String]) = {
    val spark  = SparkSession.builder()
			.appName("PerpInv")
			.enableHiveSupport()
			.getOrCreate()

    println("Processing BDAY STATUS ..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    val bday_status_df = spark.sql(" select siteid, bday, max(if(tx_type_cd = 'PMIX', is_no, 0)) pmix_is_no, max(if(tx_type_cd = 'DELI', is_no, 0)) deli_is_no, max(if(tx_type_cd = 'INVADJ', is_no, 0)) invadj_is_no, max(if(tx_type_cd = 'RESET', is_no, 0)) reset_is_no, max(if(tx_type_cd = 'STOCK', is_no, 0)) stock_is_no from perpinv.stk3_bday_tx group by siteid, bday order by siteid, bday")

    bday_status_df.write
	.format("csv")
	.mode("overwrite")
	.save("hdfs:///user/root/PerpInv/stk3_bday_status.csv")

    println("Processing end BDAY STATUS ..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    spark.stop()
  }
}

