package uk.gov.ons.sbr.utils

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SaveMode}

import uk.gov.ons.sbr.logger.SessionLogger
import uk.gov.ons.sbr.utils.FileProcessor.{CSV, Header}

object Export {
  def apply(dataFrame: DataFrame, path: Path, headerOption: Boolean = true): Unit = {
    SessionLogger.log(msg = s"Exporting Sample output to csv [$path] with length [${dataFrame.count}]")

    dataFrame
      .coalesce(numPartitions = 1)
      .write.format(CSV)
      .option(Header, headerOption)
      .mode(SaveMode.Append)
      .csv(path.toString)
  }
}
