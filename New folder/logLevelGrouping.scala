import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object logLevelGrouping extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/Lenovo/Desktop/Trendy tech/Sparkdepth/bigLog.txt")
  
  
  val mappedRdd = input.map(x => {
    val fields = x.split(":")
    (fields(0),fields(1))
  })
  
  //mappedRdd.collect()
  mappedRdd.groupByKey.collect().foreach(x=> println(x._1,x._2.size))
  
  scala.io.StdIn.readLine()
}