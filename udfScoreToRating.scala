
import org.apache.spark.sql.functions.udf

object udfScoreToRatings {
  def udfScoreToRating = udf((score: Int) => {
    score match {
      case t if t == 5 => "Excellent"
      case t if t == 4 => "Very Good"
      case t if t == 3 => "Good"
      case _ => "Needs Improvement"
    }
  })
}
