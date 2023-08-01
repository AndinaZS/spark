import Service._
import org.apache.spark.sql.functions.{col, to_date}

import java.util.Properties

object UploadData extends App {

    val cxnProp = new Properties()
    cxnProp.put("driver", "org.postgresql.Driver")
    val url =  "jdbc:postgresql://ingress-1.prod.dmp.vimpelcom.ru:5448/demo"

    val tableSeats = spark
      .read
      .jdbc(url,"bookings.seats", cxnProp)

    tableSeats.write
      .mode("overwrite")
      .format("parquet")
      .saveAsTable("school_de_stg.seats_aamarkova")

    val tableFlights = spark
      .read
      .jdbc(url, "bookings.flights_v", cxnProp)

    tableFlights
      .withColumn("departure_date", to_date(col("actual_departure"), "yyyy-MM-dd"))
      .write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("departure_date")
      .saveAsTable("school_de_stg.flights_v_aamarkova")

}