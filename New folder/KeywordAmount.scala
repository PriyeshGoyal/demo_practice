import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object KeywordAmount extends App {
  
  //here we define a function for broadcast variable
  //load balancer in which we define a set for non usable keywords
  def loadBoringWords():Set[String] = {
    var boringWords:Set[String] = Set()
    
    
    //here lines are locally stored or local machine
    val lines = Source.fromFile("/C:/Users/Lenovo/Desktop/Sparkdepth/boringwords.txt").getLines()
      for(line <- lines) {
        boringWords += line
      }
    boringWords      //return type
  }
  
 
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","ratingcalculator")
  
  var nameSet = sc.broadcast(loadBoringWords)
  
  val initial = sc.textFile("/C:/Users/Lenovo/Desktop/Sparkdepth/bigdatacampaigndata.csv")
  
  val mapInput = initial.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  
  val words = mapInput.flatMapValues(x => x.split(" "))
  
  val finalMapped = words.map(x => (x._2.toLowerCase(),x._1))
  
  //to check the value and give true or false
  val filteredRdd = finalMapped.filter(x => nameSet.value(x._1))
  
  val totals = filteredRdd.reduceByKey((x,y) => x + y )
  //val totals = finalMapped.reduceByKey((x,y) => x + y )
  
  val sorted = totals.sortBy(x => x._2,false)
  
  sorted.take(20).foreach(println)
  
  
  
}