import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object Analysis_3 extends App {
  val conf = new SparkConf()
    .setAppName("TED Talks Analytics")
    .setMaster("local")
  val sc = new SparkContext(conf)

  // Load the CSV file as an RDD
  val tedTalksRDD = sc.textFile("C:\\Users\\abhin\\OneDrive\\Desktop\\data.csv")
       //Analysis 3: Finding TED talks of your favorite author

      val favoriteAuthor = "Alex Gendler" // Replace with your favorite author's name
      val author  = tedTalksRDD
        .zipWithIndex() // Add line index to each row
        .filter { case (_, index) => index > 0 } // Skip header row
        .map { case (line, _) => line.split(",")(1) } // Assuming title is the 2 column

        val views = tedTalksRDD.zipWithIndex() // Add line index to each row
          .filter { case (_, index) => index > 0 } // Skip header row
          .map { case (line, _) =>
            val columns = line.split(",") // Assuming columns are comma-separated
            val view = if (columns(3) == "null") 0 else columns(3).toInt
            (view)}

    val title = tedTalksRDD
      .zipWithIndex() // Add line index to each row
      .filter { case (_, index) => index > 0 } // Skip header row
      .map { case (line, _) => line.split(",")(0)} // Assuming title is the 1 column

     val d = views.mean()
     val w = author.zip(views)
     val u = w.zip(title)
     val y = u.filter(a=>a._1._1.contains(favoriteAuthor))
     val z = y.filter(a=>a._1._2>=d).sortBy(_._1._2,ascending = false)
     val fin = z.map(a=> "Title: "+ a._2+" - Views: "+a._1._2)

    println(s"TED talks by $favoriteAuthor:")
    fin.foreach(println)
    println("--------------------------------------")

}
