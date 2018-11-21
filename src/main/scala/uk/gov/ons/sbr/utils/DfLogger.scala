package uk.gov.ons.sbr.utils

import org.apache.spark.sql.DataFrame

trait DfLogger {
  def logPartitionInfo(df:DataFrame, line:Int, classname:String, message:String = "") = {
    val partitions = df.rdd.getNumPartitions
    println(message)
    println(s"$classname:$line Number of partitions: $partitions")
  }
}
