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

