package uk.gov.ons.sbr.service.validation

import org.apache.spark.sql.DataFrame


case class SampleMethodArguments(
                                   stratificationDF: DataFrame,
                                   samplingProperties: DataFrame,
                                   outputTable: String
                                 ) extends MethodArguments
object SampleMethodArguments{


  def apply(args:Seq[Any]):SampleMethodArguments = args match {

    case List(
              stratificationDF:DataFrame,
              samplingProperties:DataFrame,
              outputTable: String
              ) => new SampleMethodArguments( stratificationDF,
                                              samplingProperties,
                                              outputTable
                                             )
    case _ => throw new IllegalArgumentException(
                                                  s"cannot create instance of SampleMethodArguments. Invalid arguments: \n" +
                                                  s"stratificationDF: ${args(0).getClass.getCanonicalName()};  \n"+
                                                  s"stratificationProperties: ${args(1).getClass.getCanonicalName()};  \n"+
                                                  s"stratificationFilePath: ${args(2).getClass.getCanonicalName()};   \n"+
                                                  s"outputDirectoryPath: ${args(3).getClass.getCanonicalName()};   \n"
                                                 )
     }
}
