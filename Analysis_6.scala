import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object Analysis_6 extends App {
  val conf = new SparkConf()
    .setAppName("TED Talks Analytics")
    .setMaster("local")
  val sc = new SparkContext(conf)

  // Load the CSV file as an RDD
  val tedTalksRDD = sc.textFile("C:\\Users\\abhin\\OneDrive\\Desktop\\data.csv")

// Analysis 6: Finding the most popular TED talks speaker (in terms of number of talks)
           val popularSpeakerRDD = tedTalksRDD
          .zipWithIndex() // Add line index to each row
          .filter { case (_, index) => index > 0 } // Skip header row
          .map { case (line, _) =>
            val columns = line.split(",") // Assuming columns are comma-separated
            val speaker = columns(1) // Assuming speaker is the 2nd column
            val views = if (columns(3) == "null") 0 else columns(3).toInt
            (speaker,views)
          }
          .reduceByKey(_+_)

        val speechesRDD = tedTalksRDD
          .zipWithIndex() // Add line index to each row
          .filter { case (_, index) => index > 0 } // Skip header row
          .map { case (line, _) =>
            val columns = line.split(",") // Assuming columns are comma-separated
            val speaker = columns(1) // Assuming speaker is the 2nd column
            speaker
          }
          .map(a=>(a,1))
          .reduceByKey(_ + _)

        val popRDD = popularSpeakerRDD.zip(speechesRDD)
        val weight = 1000000
        popRDD.map(a=>(a._1._1, (a._1._2/a._2._2)+(a._2._2*weight), (a._1._2/a._2._2))).sortBy(_._2, ascending = false).take(5).foreach(println)


        println("Most popular TED talks speaker:")
        println("--------------------------------------")
}
