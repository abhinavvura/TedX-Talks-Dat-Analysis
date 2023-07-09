import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object Analysis_4 extends App {
  val conf = new SparkConf()
    .setAppName("TED Talks Analytics")
    .setMaster("local")
  val sc = new SparkContext(conf)

  // Load the CSV file as an RDD
  val tedTalksRDD = sc.textFile("C:\\Users\\abhin\\OneDrive\\Desktop\\data.csv")

   //Analysis 4: Finding TED talks with the best view to like ratio
    val viewLikeRatioRDD = tedTalksRDD
      .zipWithIndex() // Add line index to each row
      .filter { case (_, index) => index > 0 } // Skip header row
      .map { case (line, _) =>
        val columns = line.split(",") // Assuming columns are comma-separated
        val title = columns(0)
        val views = if (columns(3) == "null") 0 else columns(3).toInt // Assuming views is the 4th column
        val likes = if (columns(4) == "null") 0 else columns(4).toInt // Assuming likes is the 5th column
        val ratio = if (likes != 0) views.toDouble / likes.toDouble else 0.0
        (title, ratio)
      }
      .sortBy(_._2, ascending = false)
      .take(10)

    println("TED talks with the best view to like ratio:")
    viewLikeRatioRDD.foreach(println)
    println("--------------------------------------")
}