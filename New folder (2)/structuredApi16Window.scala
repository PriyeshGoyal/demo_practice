import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum


case class Data(country: String, weeknum: Int, numinvoices: Int, totalqauntity: Int, invoicevalue: Double )

object structuredApi16Window extends App {
 
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  //creating spark conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  //creating spark session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  val invoiceDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/windowdata.csv")
  .load()
  
  
  val df1 = invoiceDf.toDF("country","weeknum","numinvoices","totalquantity","invoicevalue")
  
  
  df1.printSchema()

  val myWindow = Window.partitionBy("country")
  .orderBy("weeknum")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  
  val myDf =  df1.withColumn("RunningTotal", sum("invoicevalue").over(myWindow))
  
  myDf.show()     
     
}