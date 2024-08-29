import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions._

object structureApi20 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  //creating spark conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  //creating spark session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  val orderDf = spark.read
  .format("csv")
  .option("inferSchema",true)
  .option("header", true)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/orders.csv")
  .load()
  
  val customerDf = spark.read
  .format("csv")
  .option("inferSchema",true)
  .option("header", true)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/customers.csv")
  .load()
  
  val orderNew = orderDf.withColumnRenamed("customer_id", "cust_id")
  //customerDf.show()
  
  //Join Condition
  val joinCondition = orderNew.col("cust_id") === customerDf.col("customer_id")
  
  //join Type
  val joinType = "outer"
  
  
  //joining 2 dataframes
  val joinDf = orderNew.join(customerDf, joinCondition, joinType)
  
  //broadcast join
  //val joinDf = orderNew.join(broadcast(customerDf), joinCondition, joinType)
  
  .select("order_id","customer_id","customer_fname")
  .sort("order_id")
  
  .withColumn("order_id",expr("coalesce(order_id,-1)"))
  
  //.drop(orderDf.col("customer_id"))   //drop a column to deal with a ambiguity  
  
  .show(1000)
  
}