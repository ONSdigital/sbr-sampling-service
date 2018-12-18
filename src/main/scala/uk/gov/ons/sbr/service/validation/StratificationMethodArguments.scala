package uk.gov.ons.sbr.service.validation


import org.apache.spark.sql.DataFrame

case class StratificationMethodArguments(unitFrame: DataFrame,
                                         stratificationProperties: DataFrame,
                                         hiveTable: String,  //this is fully qualified table name with db name prepended
                                         unit:String,
                                         bounds:String
                                        ) extends MethodArguments
object StratificationMethodArguments {


  def apply(args:Seq[Any]):StratificationMethodArguments = args match {

    case List(
              unitFrame:DataFrame,
              stratificationProperties:DataFrame,
              outputHiveTableName:String,
              unit:String,
              bounds:String
            ) => new StratificationMethodArguments(  unitFrame,
                                                      stratificationProperties,
                                                      outputHiveTableName,
                                                      unit,
                                                      bounds
                                                  )
    case _ => throw new IllegalArgumentException(
      s"cannot create instance of SampleMethodArguments. Invalid arguments:\n" +
        s"stratificationDF: ${args(0).getClass.getCanonicalName()};\n"+
        s"stratificationProperties: ${args(1).getClass.getCanonicalName()};\n"+
        s"outputTable: ${args(2).getClass.getCanonicalName()};\n"+
        s"unit: ${args(3).getClass.getCanonicalName()};\n" +
        s"bound: ${args(4).getClass.getCanonicalName()};\n"
    )
  }
}
