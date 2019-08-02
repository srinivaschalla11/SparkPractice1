package com.practice.spark

//import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class Family(id:Integer,name:String,salary:Float)
/*
*  To create DataSet first we should know about data like what columns and their types
*  does data is having, so we can create case class for that columns and by using as[]
*  we can create DATASET. While using as[] we have to import spark.implicit._ in our code.
* */


object _5Creating_DataSet {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Creating_Dataset")
      .master("local")
      .getOrCreate()

    val schemaDS = StructType(
      StructField("ID",IntegerType,false)::
      StructField("NAME",StringType,false)::
      StructField("Salary", FloatType,false):: Nil
    )

    import spark.implicits._

    val csvDS = spark.read
      .option("inferSchema",true)
      .schema(schemaDS)
      .csv("/home/hduser/data_sets/family.csv").as[Family]

    /*
    * In above we have used options("inferSchema",true) if we dont specify that
    * default schma will be consider as String and while every line in csv file is
    * made as object for family class. So family case class members have datatypes
    * so we will get errors. So instead of converting String datatype to case calss
    * data type we can use "inferschema".
    * */

    csvDS.createOrReplaceTempView("family")

    spark.sql("select * from family where NAME ='srinivas'").show()


  }

}
