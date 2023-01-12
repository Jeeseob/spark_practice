package accumulatorExample

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.Unit

object AccumulatorExample {
  private def accChinaFunc(flight_row: Flight, accChina: LongAccumulator): Unit = {
    val destination = flight_row.DEST_COUNTRY_NAME
    val origin = flight_row.ORIGIN_COUNTRY_NAME
    if (destination == "China") {
      accChina.add(flight_row.count.toLong)
    }
    if (origin == "China") {
      accChina.add(flight_row.count.toLong)
    }
  }
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("accumulatorExample")
      .getOrCreate()

    val flights = spark.read
      .parquet("./resources/2010-summary.parquet/")
      .as[Flight](Encoders.product[Flight])
    flights.show()

    Thread.sleep(1000)

    val accChina = new LongAccumulator
    spark.sparkContext.register(accChina, "China")
    flights.foreach(flight => accChinaFunc(flight, accChina))
    println(accChina.value) // 953

    Thread.sleep(1000)

    // Custom Accumulator
    val acc = new EvenAccumulator
    spark.sparkContext.register(acc, "evenAcc")
    flights.foreach(flight => acc.add(flight.count))
    println(acc.value()) // 31390


    Thread.sleep(100000)
  }
}
