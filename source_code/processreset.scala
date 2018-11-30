package com.poc.examples
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object processreset {
  def main(args: Array[String]) = {
    val spark  = SparkSession.builder()
			.appName("PerpInv")
			.enableHiveSupport()
			.getOrCreate()

    println("Processing RESET stk3_tx..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    val tx_df = spark.sql("select a.is_no, a.siteid, a.pponumber, a.trx_date bday, a.reset_qty_units qty from perpinv.stg_reset a where a.siteid = 11884")

    tx_df.write
	.format("csv")
	.mode("overwrite")
	.save("hdfs:///user/root/PerpInv/reset_stk3_tx.csv")

    println("Processing end RESET stk3_tx..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    println("Processing RESET stk3_bday_tx..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    val bday_tx_df = spark.sql("select distinct a.is_no, a.siteid, a.trx_date bday from perpinv.stg_reset a where a.siteid = 11884")

    bday_tx_df.write
	.format("csv")
	.mode("overwrite")
	.save("hdfs:///user/root/PerpInv/reset_stk3_bday_tx.csv")

    println("Processing end RESET stk3_bday_tx..." +  DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss")
		.format(LocalDateTime.now))

    spark.stop()
  }
}

