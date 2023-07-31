import org.apache.spark.sql.SparkSession

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

}
