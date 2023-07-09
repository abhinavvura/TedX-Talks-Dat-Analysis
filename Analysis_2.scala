
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try


object Analysis_2 {
  val conf = new SparkConf()
    .setAppName("TED Talks Analytics")
    .setMaster("local")
  val sc = new SparkContext(conf)


  // Load the CSV file as an RDD
  val tedTalksRDD = sc.textFile("C:\\Users\\abhin\\OneDrive\\Desktop\\data.csv")

  // Analysis 2: Finding TED talks based on tags (e.g., climate)
  val tag = "climate" // Replace with your desired tag
  val title = tedTalksRDD
    .zipWithIndex() // Add line index to each row
    .filter { case (_, index) => index > 0 } // Skip header row
    .filter { case (line, _) => line.toLowerCase.contains(tag) }
    .map { case (line, _) => line.split(",")(0) } // Assuming title is the 1st column


  val occupation = tedTalksRDD
    .zipWithIndex() // Add line index to each row
    .filter { case (_, index) => index > 0 } // Skip header row

    .map { case (line, _) => line.split(",")(6) } // Assuming job is the 7 column

  val x = title.zip(occupation)
  val p = x.filter(a => (a._1).contains(tag))
  val finals = p.map(x => "Title - " + x._1 + ", Occupation - " + x._2).foreach(println)
}