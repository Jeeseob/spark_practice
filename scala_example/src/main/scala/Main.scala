import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile("README.md")
    val counts = textRDD.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      //shuffle
      .reduceByKey(_ + _)

    val countsCache = counts.cache()
    countsCache.collect()
    countsCache.foreach(tuple => println(tuple._1, tuple._2))

    println("--------------------------------------------------")

    val sortedRDD = countsCache.map(tuple => (tuple._2, tuple._1))
      //shuffle
      .sortByKey(false)
      .map(tuple => (tuple._2, tuple._1))
      .foreach(tuple => println(tuple._1, tuple._2))
  }
}