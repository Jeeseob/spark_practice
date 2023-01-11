import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.desc
object ReadCSV {
  def main(args: Array[String]): Unit = {
    while(true) {
      val spark = SparkSession
        .builder()
        .master("local")
        .appName("readCSV")
        .getOrCreate()

      val flightDataFrame = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./resources/2015-summary.csv")


      flightDataFrame
        .groupBy("DEST_COUNTRY_NAME")
        .sum("count")
        .withColumnRenamed("sum(count)", "destination_total")
        .sort(desc("destination_total"))
        .limit(5)
        .explain()

      val flightDataFrameCached = flightDataFrame.cache()
      //    flightDataFrameCached.show()

      flightDataFrameCached
        .groupBy("DEST_COUNTRY_NAME")
        .sum("count")
        .withColumnRenamed("sum(count)", "destination_total")
        .sort(desc("destination_total"))
        .limit(5)
        .explain()

      /**
       * shell에서는 line by line으로 caching을 하지만,
       * 전체 코드를 한번에 실행시킬 땐, 그렇게 하지 않기 때문에
       * caching을 하는 부분 InMemoryRelation, InMemoryTableScan의 존재여부가 달라지는 것.
       */
      Thread.sleep(10000)
    }
  }
}
