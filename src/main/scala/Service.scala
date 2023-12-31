import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Service {

  val spark = SparkSession
    .builder()
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()

  val tickets = spark.table("school_de.bookings_tickets")
  val ticket_flights = spark.table("school_de.bookings_ticket_flights")
  val flightsV = spark.table("school_de.bookings_flights_v")
  val airports = spark.table("school_de.bookings_airports")
  val routes = spark.table("school_de.bookings_routes")
  val aircrafts = spark.table("school_de.bookings_aircrafts")

  val toDuration = (duration: String) => {
    val res = duration.split(":")
      .map(x => x.replaceAll("[^0-9]", "")
        .toInt).reverse.zip(Array(1, 60, 3600, 86400))
      .map(x => x._1 * x._2).sum
    res
  }

  val toDurationUDF = udf(toDuration)

}
