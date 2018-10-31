package uk.gov.ons.sbr.service.validation

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

case class SampleMethodsArguments(
                                   unitFrame: DataFrame,
                                   stratificationProperties: DataFrame,
                                   outputDirectory: Path,
                                   unit:String,
                                   bounds:String
                                 )
object SampleMethodsArguments{
  def apply(args:Seq[Any]) = args match {

    case List(unitFrame:DataFrame,
              stratificationProperties:DataFrame,
              outputDirectoryPath:Path,
              unit:String,
              bounds:String
              ) => new SampleMethodsArguments(unitFrame,
                                              stratificationProperties,
                                              outputDirectoryPath,
                                              unit,
                                              bounds
                                             )
    case _ => throw new IllegalArgumentException(s"cannot create instance of SampleMethodsArguments. Invalid arguments: " +
      s"unitFrame: ${args(0).getClass.getCanonicalName()};  \n"+
      s"stratificationProperties: ${args(1).getClass.getCanonicalName()};  \n"+
      s"outputDirectoryPath: ${args(2).getClass.getCanonicalName()};   \n"+
      s"unit: ${args(3).getClass.getCanonicalName()};   \n"+
      s"bounds: ${args(4).getClass.getCanonicalName()};  "
    )
     }
}
