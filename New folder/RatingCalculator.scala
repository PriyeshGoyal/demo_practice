import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object RatingCalculator extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","ratingcalculator")
  
  val input = sc.textFile("/C:/Users/Lenovo/Downloads/moviedata.data")
  
  val mappedInput = input.map(x => x.split("\t")(2))    //here we only need a rating-column so we use indexing as array of 2(rating-column)
  
  val results = mappedInput.countByValue()
  
  //val ratings = mappedInput.map(x => (x,1))
  
  //val reduceRating = ratings.reduceByKey((x,y) => x+y)
  
  //val results = reduceRating.collect //paired Rdd Array
  
  results.foreach(println)
}