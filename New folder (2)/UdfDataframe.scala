import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

case class Person(name: String, age: Int, city: String)

object UdfDataframe extends App {
  
  
  def ageCheck(age:Int):String = {
    if(age > 18) "Y" else "N"
    }
  
  
  //creating spark conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  //creating spark session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //reading a file using dataframe reader API
  val Df = spark.read
  .format("csv")
  //.option("header",true)
  .option("inferSchema",true)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/dataset1")
  .load()
  
  //Df.printSchema()
  import spark.implicits._
  
  //val df1 = Df.toDF("name","age","city")     //DataFrame
  
  val df1: Dataset[Row] = Df.toDF("name","age","city")   //converting DataFrame to Dataset
   
  //df1.printSchema()
  
 // val ds1 = df1.as[Person]  // converted dataframe to dataset by case class
  
 // val df2 =  ds1.toDF()   //again converting dataset to dataframe
  
  
  
  
  //column object expression udf
  /* val parseAgeFunction = udf(ageCheck(_:Int):String)
  
  val df2 = df1.withColumn("adult",parseAgeFunction(col("age")))
  
  df2.show()*/
  
  
  //  sql|string expression udf   , adding col adult and value y and n
  spark.udf.register("parseAgeFunction",ageCheck(_:Int):String)
  
  val df2 = df1.withColumn("adult",expr("parseAgeFunction(age)"))
  
  df2.show
  
  spark.catalog.listFunctions().filter(x => x.name == "parseAgeFunction").show()
  
  df1.createOrReplaceTempView("peopletable")
  
  spark.sql("select name, age, city, parseAgeFunction(age) as adult from peopletable").show
  
  
  
  
}