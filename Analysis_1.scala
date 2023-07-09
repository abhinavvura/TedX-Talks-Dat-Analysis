import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object bda extends App {
  val conf = new SparkConf()
    .setAppName("TED Talks Analytics")
    .setMaster("local")
  val sc = new SparkContext(conf)

  // Load the CSV file as an RDD
  val tedTalksRDD = sc.textFile("C:\\Users\\abhin\\OneDrive\\Desktop\\data.csv")



  // Analysis 1: Finding the most popular TED talks based on views
    val popularTalksRDD = tedTalksRDD
      .zipWithIndex() // Add line index to each row
      .filter { case (_, index) => index > 0 } // Skip header row
      .map { case (line, _) =>
        val columns = line.split(",") // Assuming columns are comma-separated
        val title = columns(0)
        val views = if (columns(3) == "null") 0 else columns(3).toInt // Assuming views is the 4th column
        (views, title) // Swap views and title for sorting purposes
      }
      val top=popularTalksRDD.sortByKey(ascending = false).take(10).sort
      val least = popularTalksRDD.sortByKey().take(10)


      println("Analysis - 1-----> Most popular TED talks based on views:")
      val p = top.foreach(println)// Get the top 10 talks based on views
      // Extract only the title


    println("Analysis - 1-----> Least popular TED talks based on views:")
    val q = least.foreach(println) // Get the top 10 talks based on views
    println("--------------------------------------")

    //No of above avg talks wrt views
    val a = tedTalksRDD.zipWithIndex() // Add line index to each row
      .filter { case (_, index) => index > 0 } // Skip header row
      .map { case (line, _) =>
        val columns = line.split(",") // Assuming columns are comma-separated
        val title = columns(0)
        (title)

      }
    val b = tedTalksRDD.zipWithIndex() // Add line index to each row
      .filter { case (_, index) => index > 0 } // Skip header row
      .map { case (line, _) =>
        val columns = line.split(",") // Assuming columns are comma-separated

        val views = if (columns(3) == "null") 0 else columns(3).toInt
        (views)
      }

    val c = tedTalksRDD.zipWithIndex() // Add line index to each row
      .filter { case (_, index) => index > 0 } // Skip header row
      .map { case (line, _) =>
        val columns = line.split(",") // Assuming columns are comma-separated

        val speaker = columns(1)
        (speaker)
      }

    val d = b.mean()
    val x = a.zip(b)
    val y = x.filter(a=>(a._2>=d))
    println("Above avg talks wrt views--->" + y.count() )
    val abovavg = y.foreach(println)

    val speak = c.zip(b)
    val o = speak.filter(a=>(a._2>=d)).map(a=>(a._1,1)).reduceByKey(_+_).sortBy(_._2 ,ascending = false).foreach(println)

  sc.stop()

}