import org.apache.spark.sql.SparkSession

object BroadCastExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("broadCastExample")
      .getOrCreate()

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)

    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,
      "Big" -> 300, "Simple" -> 100)

    val wordsWithNoneBroadcast = words.map(word => (word, supplementalData.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2, ascending = false)
      .collect()

    wordsWithNoneBroadcast.foreach(word => println(word))

    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)
    val wordsWithBroadcast = words.map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2, ascending = false)
      .collect()

    wordsWithBroadcast.foreach(word => println(word))
  }
}