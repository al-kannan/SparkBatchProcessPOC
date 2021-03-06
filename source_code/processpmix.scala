package com.poc.examples
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object processpmix {
  def main(args: Array[String]) = {
    val spark  = SparkSession.builder()
			.appName("PerpInv")
			.enableHiveSupport()
			.getOrCreate()

    println("Processing PMIX stk3_tx..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    val tx_df = spark.sql("select a.is_no, a.siteid, b.pponumber, a.trx_date bday, a.sales_qty_units * b.usage_qty qty from perpinv.stg_pmix a inner join perpinv.stg_bmitoppo b on b.siteid = a.siteid and b.menuitem = a.menuitem where a.siteid = 11884 and b.siteid = 11884")

    tx_df.write
	.format("csv")
	.mode("overwrite")
	.save("hdfs:///user/root/PerpInv/pmix_stk3_tx.csv")

    println("Processing end PMIX stk3_tx..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    println("Processing PMIX stk3_bday_tx..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    val bday_tx_df = spark.sql("select distinct a.is_no, a.siteid, a.trx_date bday from perpinv.stg_pmix a where a.siteid = 11884")

    bday_tx_df.write
	.format("csv")
	.mode("overwrite")
	.save("hdfs:///user/root/PerpInv/pmix_stk3_bday_tx.csv")

    println("Processing end PMIX stk3_bday_tx..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    spark.stop()
  }
}

