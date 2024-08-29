import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object dataFrameExample extends App {
  
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  
 val spark = SparkSession.builder()
  //.appName("My Application 1")
  //.master("local[2]")
  .config(sparkConf)
  .getOrCreate()
  
  //processing
  
  val ordersDf = spark.read
  .option("header",true)
  .option("inferSchema",true)
  .csv("C:/Users/Lenovo/Desktop/Trendy tech/Sparkdepth/Spark_week11/orders.csv")
  
  val groupOrdersDf = ordersDf
  .repartition(4)
  .where("order_customer_id > 10000")
  .select("order_id","order_customer_id")
  .groupBy("order_customer_id")
  .count()
  
  //ordersDf.show()
  
  groupOrdersDf.show()
  
  groupOrdersDf.foreach( x=> {
    println(x)
  })
  
  //ordersDf.printSchema()
  
  //scala.io.StdIn.readLine()
  
  spark.stop()
  
  scala.io.StdIn.readLine()

}