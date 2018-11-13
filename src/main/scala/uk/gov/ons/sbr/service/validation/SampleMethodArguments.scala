package uk.gov.ons.sbr.service.validation

import org.apache.spark.sql.DataFrame


case class SampleMethodArguments(
                                   stratificationDF: DataFrame,
                                   samplingProperties: DataFrame,
                                   outputDbName: String,
                                   outputTableName: String
                                 ) extends MethodArguments
object SampleMethodArguments{


  def apply(args:Seq[Any]):SampleMethodArguments = args match {

    case List(
              stratificationDF:DataFrame,
              samplingProperties:DataFrame,
              outputDbName: String,
              outputTableName: String
              ) => new SampleMethodArguments( stratificationDF,
                                              samplingProperties,
                                              outputDbName,
                                              outputTableName
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
