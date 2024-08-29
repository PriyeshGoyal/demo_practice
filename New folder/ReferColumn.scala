import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ReferColumn extends App{
 
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  

  
  val orderDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/orders.csv")
  .load()
 
  //orderDf.select("order_id","order_status").show()   //column string format
  
  import spark.implicits._
  
  orderDf.select(column("order_id"),col("order_date"),$"order_customer_id",'order_status).show()   //column object format
  
  //orderDf.select("order_id","concat(order_status,'_STATUS')").show()    //column expression
  
  //orderDf.select(column("order_id"),expr("concat(order_status,'_STATUS')")).show()   //column expression with col object
  
  orderDf.selectExpr("order_id","order_date","concat(order_status,'_STATUS')").show() //with col string col expression
  
  spark.stop()

}

