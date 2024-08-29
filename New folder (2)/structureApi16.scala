import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum


object structureApi16  extends App {
  
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
  
  
  // SIMPLE AGGREGATES
  
  //using colummn object expression
  invoiceDf.select(
      count("*").as("RowCount"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgPrice"),
      countDistinct("InvoiceNo").as("CountDistinct")
  ).show()
  
  //USING string expression
  invoiceDf.selectExpr(
      "count(StockCode) as RowCount",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgPrice",
      "count(Distinct(InvoiceNo)) as countDistinct"

  ).show() 
  
  
  //using spark sql
  invoiceDf.createOrReplaceTempView("sales")
  spark.sql("select count(*), sum(Quantity), avg(UnitPrice), count(Distinct(InvoiceNo)) from sales")
  .show()
}