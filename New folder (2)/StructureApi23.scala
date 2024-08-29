import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object StructureApi23 extends App {
  
  
  case class Logging(level: String, datetime: String)
  
    def mapper(line:String): Logging = {
    val fields = line.split(',')
 
    val logging: Logging = Logging(fields(0), fields(1))
    return logging
  }
  
   //def main(args: Array[String]) {
  
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  //creating spark conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  //creating spark session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
   
  import spark.implicits._
  
  val myList = List("WARN, 2016-12-31 04:19:32",
                    "FATAL, 2016-12-31 03:22:34",
                    "WARN, 2016-12-31 03:21:21",
                    "INFO, 2015-04-21 14:32:21",
                    "FATAL, 2015-04-21 19:23:20")  
  
  val rdd1 = spark.sparkContext.parallelize(myList)
  
  val rdd2 = rdd1.map(mapper)
  
  val df1 = rdd2.toDF()
  
  df1.createOrReplaceTempView("logging_table")
  
  //spark.sql("select * from logging_table").show()
 
   
  //spark.sql("select level, count(datetime) from logging_table group by level order by level ").show()
  
  
  /* val df2 = spark.sql("select level, date_format(datetime,'MMMM') as month from logging_table")
  
  df2.createOrReplaceTempView("new_logging_table")

  spark.sql("select level , month, count(1) from new_logging_table group by level, month")
  
  .show()
  
  */
  
  
  val df3 = spark.read
  .option("header", true)
  .csv("C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/biglog.txt")
  
  //df3.show()
  
  df3.createOrReplaceTempView("my_new_logging_table")
  
  
  val results =  spark.sql("""select level, date_format(datetime, 'MMMM') as month ,count(1) as total from my_new_logging_table
     group by level, month""")
 
 // results.createOrReplaceTempView("results_table")
  
  /* val result1 = spark.sql("""select level, date_format(datetime, 'MMMM') as month ,
    cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total 
    from my_new_logging_table
    group by level, month order by monthnum , level""")
    
   val result2= result1.drop("monthnum")
  
   result2.show(60)
   
   */
  
     
    /*
    //pivot table
   val result3 = spark.sql(""""select level, date_format(datetime, 'MMMM') as month ,
    cast(date_format(datetime, 'M') as int) as monthnum 
    from my_new_logging_table""").groupBy("level").pivot("monthnum").count()
    
    */
  
  
  val columns = List("January", "February", "March", "April", "May", "June", "July", "August", "September", "November", 
         "December")
  
   val result4 = spark.sql("""select level, date_format(datetime, 'MMMM') as month 
    from my_new_logging_table""").groupBy("level").pivot("month", columns).count().show(100)
  
}
