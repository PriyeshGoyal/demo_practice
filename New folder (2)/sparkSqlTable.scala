import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Level
import org.apache.log4j.Logger


object sparkSqlTable extends App{

  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","SqlSparkTbale")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()   //to enabling hive support
  .getOrCreate()
  
  val orderssDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/orders.csv")
  .load()
  
  spark.sql("create database if not exists retail")
  
 // val ordersRep = orderssDf.repartition(4)
  
  orderssDf.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .bucketBy(4, "order_customer_id")
  .sortBy("order_customer_id")
  //.saveAsTable("orders5")
  .saveAsTable("retail.orders")
  
  spark.catalog.listTables("retail").show()
  
  
  scala.io.StdIn.readLine()
  spark.close()
  
}