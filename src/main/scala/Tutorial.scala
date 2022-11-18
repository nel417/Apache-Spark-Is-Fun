import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.LongType
import Utilities._


object Tutorial extends App {

  Utilities.setupLogging()

  val spark = SparkSession
    .builder()
    .appName("YelpReviews")
    .master("local[*]")
    .getOrCreate()

//  val firstDataFrame = spark.read
//    .format("json")
//    .load("data/InstrumentReviews.json")


  val secondDataFrame = spark.read
    .schema(yelpReviewSchema)
    .format("json")
    .load("data/yelp.json")

//  firstDataFrame.show()
//  firstDataFrame.printSchema()

  secondDataFrame.show()
  secondDataFrame.printSchema()
  def getName(): Unit = {
    secondDataFrame.select("name").show()

  }

 print(getName())
}