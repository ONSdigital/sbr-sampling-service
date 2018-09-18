package uk.gov.ons.sbr.service.validation

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

case class SampleMethodsArguments(
                               unitFrame: DataFrame,
                               stratificationProperties: DataFrame,
                               outputDirectory: Path
                             )
