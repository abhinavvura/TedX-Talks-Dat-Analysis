import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object Analysis_7 extends App {
  val conf = new SparkConf()
    .setAppName("TED Talks Analytics")
    .setMaster("local")
  val sc = new SparkContext(conf)

  // Load the CSV file as an RDD
  val tedTalksRDD = sc.textFile("C:\\Users\\abhin\\OneDrive\\Desktop\\data.csv")

  // Analysis 7
  val liked = "The story behind the Mars Rovers" //Can give desired title
  val tags = liked.split(" ")
  val stopwords = List("a", "an", "the", "in", "on", "is", "The", "behind")
  val n_tags = tags.filterNot(stopwords.contains)
  val title = tedTalksRDD
    .zipWithIndex() // Add line index to each row
    .filter { case (_, index) => index > 0 }
    .map { case (line, _) => line.split(",")(0) }


  val speaker = tedTalksRDD
    .zipWithIndex() // Add line index to each row
    .filter { case (_, index) => index > 0 }
    .map { case (line, _) => line.split(",")(1) }

  n_tags.foreach { tag =>
    val x = title.zip(speaker)
    val p = x.filter(a => (a._1).contains(tag)).filter(a => a._1 != liked)
    val q = p.map(a => (a._2, 1)).reduceByKey(_ + _).foreach(println)
    println(tag)
  }
}
