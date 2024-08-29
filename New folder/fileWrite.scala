import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession



object fileWrite extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","writeFile")
  sparkConf.set("spark.master","local[2]")
 

  //sparkConf.set("spark.speculation","false")
  
  
  val spark =  SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()

  
  val orderDf = spark.read
  .format("CSV")
  .option("header",true)
  .option("inferSchema",true)
  .option("mode","DROPMALFORMED")
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/customers.csv")
  .load()
  
  //print("orderdf has " + orderDf.rdd.getNumPartitions)
  
  
  val orderRep = orderDf.repartition(2)
  
  print("orderrep has " + orderRep.rdd.getNumPartitions)
  //ordersDf.show()
  
  orderRep.show()
  
  orderDf.write
  //orderRep.write
  .format("JSON")
  //.partitionBy("order_status")
  .mode(SaveMode.Overwrite)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/customer")
  //.option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/newfolder")
  .save()
  
  scala.io.StdIn.readLine()
  spark.stop()
  //scala.io.StdIn.readLine()
}