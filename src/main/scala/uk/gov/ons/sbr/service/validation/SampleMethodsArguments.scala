package uk.gov.ons.sbr.service.validation

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

case class SampleMethodsArguments(
                                   unitFrame: DataFrame,
                                   stratificationProperties: DataFrame,
                                   outputDirectory: Path,
                                   unitSpecDF: DataFrame
                                 )
object SampleMethodsArguments{
  def apply(args:Seq[Any]) = args match {

    case List(unitFrame:DataFrame,
              stratificationProperties:DataFrame,
              outputDirectoryPath:Path,
              unitSpecDF:DataFrame
              ) => new SampleMethodsArguments(unitFrame,
                                              stratificationProperties,
                                              outputDirectoryPath,
                                              unitSpecDF
                                             )
    case _ => throw new IllegalArgumentException("cannot create instance of SampleMethodsArguments. Invalid arguments")
     }
}
