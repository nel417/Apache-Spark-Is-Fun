
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType}
object Utilities {
  def setupLogging() = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  }
  val instrumentReviewsSchema = StructType(Array(
    StructField("reviewerId", StringType, nullable = true),
    StructField("reviewerName", StringType, nullable = true),
    StructField("reviewText", StringType, nullable = true),
    StructField("overall", DoubleType, nullable = true),
    StructField("summary", StringType, nullable = true),
  ))

  /*
   * Write your Yelp Schema below
   */

  val yelpReviewSchema = StructType(Array(
    StructField("name", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("stars", DoubleType, nullable = true),
    StructField("review_count", IntegerType, nullable = true),
    StructField("state", StringType, nullable = true),
    StructField("hours", StructType(
      Array(
        StructField("Monday", StringType),
        StructField("Tuesday", StringType),
        StructField("Wednesday", StringType),
        StructField("Thursday", StringType),
        StructField("Friday", StringType),
        StructField("Saturday", StringType),
        StructField("Sunday", StringType)
      )
    ))
  ))
}
