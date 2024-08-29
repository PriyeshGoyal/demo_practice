import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._



object structureApi15 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  //creating spark conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  //creating spark session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  val myList =   List((1,"2013-07-25",11599,"CLOSED"),
      (2,"2013-07-25" ,256,"PENDING_PAYMENT"),
      (3,"2013-07-25" ,12111,"COMPLETE"),
      (4,"2013-07-25" ,8827,"CLOSED"),
      (5,"2013-07-25" ,11318,"COMPLETE"),
      (6,"2013-07-25" ,7130,"COMPLETE"))
  
  val ordersDf = spark.createDataFrame(myList)   //convert list to dataframe
  .toDF("orderid","orderdate","customerid","status")
  
  val newDf = ordersDf
  .withColumn("orderdate",unix_timestamp(col("orderdate")
  .cast(DateType)))
  .withColumn("newid", monotonically_increasing_id)
  .dropDuplicates("orderdate","customerid")
  //.drop("orderid")
  .sort("orderdate","newid")
  
  
  newDf.printSchema()
  
  newDf.show()
 
  spark.stop()
}