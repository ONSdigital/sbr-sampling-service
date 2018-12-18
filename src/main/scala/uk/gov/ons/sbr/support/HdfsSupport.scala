package uk.gov.ons.sbr.support

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object HdfsSupport {
  // TODO - FileSystem.get throws Exception
  def exists(path: Path)(implicit sparkSession: SparkSession): Boolean = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(path)
  }
}
