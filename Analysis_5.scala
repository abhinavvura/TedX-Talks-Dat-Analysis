import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object Analysis_5 extends App {
  val conf = new SparkConf()
    .setAppName("TED Talks Analytics")
    .setMaster("local")
  val sc = new SparkContext(conf)

  // Load the CSV file as an RDD
  val tedTalksRDD = sc.textFile("C:\\Users\\abhin\\OneDrive\\Desktop\\data.csv")

  // Analysis 5: Month-wise analysis of TED talk frequency
  val monthWiseAnalysisRDD = tedTalksRDD
    .zipWithIndex() // Add line index to each row
    .filter { case (_, index) => index > 0 } // Skip header row
    .map { case (line, _) =>
      val columns = line.split(",") // Assuming columns are comma-separated
      val date = columns(2) // Assuming date is the 3rd column
      val month = date.split(" ")(0) // Extract month from date
      (month, 1)
    }
    .reduceByKey(_ + _)
    .sortByKey()

  println("Month-wise analysis of TED talk frequency:")
  monthWiseAnalysisRDD.foreach(println)
  println("--------------------------------------")

  val monthRDD = tedTalksRDD
    .zipWithIndex() // Add line index to each row
    .filter { case (_, index) => index > 0 } // Skip header row
    .map { case (line, _) =>
      val columns = line.split(",") // Assuming columns are comma-separated
      val date = columns(2) // Assuming date is the 3rd column
      val month = date.split("-")(0) // Extract month from date
      (month, 1)
    }
    .reduceByKey(_ + _)
    .sortByKey()
    .foreach(println)

  val yearRDD = tedTalksRDD
    .zipWithIndex() // Add line index to each row
    .filter { case (_, index) => index > 0 } // Skip header row
    .map { case (line, _) =>
      val columns = line.split(",") // Assuming columns are comma-separated
      val date = columns(2) // Assuming date is the 3rd column
      val year = date.split("-")(1) // Extract year from date
      (year, 1)
    }
    .reduceByKey(_ + _)
    .sortByKey()
    .foreach(println)
}
