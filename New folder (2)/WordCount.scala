import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object WordCount extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //val sc = new SparkContext("local[*]","wordcount")
  
  val sc = new SparkContext()
  
  //val input = sc.textFile("/C:/Users/Lenovo/Downloads/search_data.txt")
  
  val input = sc.textFile("s3://priyesh-goyal/search_data.txt")
  
  val words = input.flatMap(x => x.split(" "))  //we get seprate words
  
  val wordMap = words.map(x => (x,1))  //one input equals one output in key value 
  
  val finalCount = wordMap.reduceByKey((a,b) => a + b)
  
  finalCount.collect.foreach(println)
  
  
}