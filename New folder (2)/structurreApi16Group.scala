import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions._

object structurreApi16Group extends App {
  
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
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/order_data.csv")
  .load()
  
  
  //by column object expression
  val summaryDf = invoiceDf.groupBy("Country","InvoiceNo")
  .agg(sum("Quantity").as("TotalQuantity"),
   sum(expr("Quantity * UnitPrice")).as("InvoiceNo"))
   
   summaryDf.show()
   
   
   
   
   //by string expression
   val summaryDf1 = invoiceDf.groupBy("Country","InvoiceNo")
   .agg(expr("sum(Quantity) as TotalQuantity"),
       expr("sum(Quantity * UnitPrice) as InvoiceValue")
       )
      
    summaryDf.show  
    
    //by spark sql
    invoiceDf.createOrReplaceTempView("sales")
    
   val summaryDf2 =  spark.sql("""select Country, InvoiceNo , sum(Quantity) as TotalQuantity, 
      sum(Quantity * UnitPrice) as InvoiceValue from sales 
      group by Country, InvoiceNo""")
      
   summaryDf2.show()   
}