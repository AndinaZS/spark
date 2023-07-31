import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import Service._

object Queries extends App {

  import spark.implicits._

  tickets.persist()

  val df = tickets
    .groupBy('book_ref)
    .agg(count('book_ref).as("count"))

  df.persist()

  //  1. Вывести максимальное количество человек в одном бронировании
  val df1 = df.agg(max('count))
  val max_people = df1.first()(0)

  //  2. Вывести количество бронирований с количеством людей больше среднего значения
  //    людей на одно бронирование

  val avg_ = df.select(avg('count).alias("avgN")).first()(0)
  val df2 = df.filter('count >= avg_)


  /*3. Вывести количество бронирований, у которых состав пассажиров повторялся два и более
  раза, среди бронирований с максимальным количеством людей (п.1)?*/
  val windowSpec = Window.partitionBy("passenger_id")

  val df3_1 = df.filter('count === max_people).select('book_ref)

  val df3_2 = tickets
    .select('book_ref, 'passenger_id)
    .withColumn("count", count(col("passenger_id")).over(windowSpec))
    .select('book_ref, 'passenger_id).filter('count > 1)

  val df3 = df3_2.join(df3_1, "book_ref")
    .groupBy('book_ref)
    .agg(sort_array(collect_list('passenger_id)).as("passengerList"))
    .groupBy('passengerList).agg(count('passengerList).alias("count"))
    .filter('count > 1)


  //4. Вывести номера брони и контактную информацию по пассажирам в брони (passenger_id,
  //  passenger_name, contact_data) с количеством людей в брони = 3

  val df4_1 = df.filter('count === 3).select('book_ref)

  val df4 = tickets
    .select('book_ref, concat('passenger_id, lit("|"),
      'passenger_name, lit("|"), 'contact_data))
    .join(df4_1, "book_ref")

  df.unpersist()

  //  5. Вывести максимальное количество перелётов на бронь


  val df5_1 = ticket_flights
    .groupBy('ticket_no)
    .agg(count('ticket_no).alias("ticket_count"), sum('amount).alias("amount"))

  df5_1.persist()

  val df5 = df5_1.join(
      tickets.select('book_ref, 'ticket_no), "ticket_no")
    .groupBy('book_ref)
    .agg(sum('ticket_count).alias("count"))
    .agg(max('count))

  // 6. Вывести максимальное количество перелётов на пассажира в одной брони

  val df6 = df5_1.join(
      tickets.select('book_ref, 'ticket_no, 'passenger_id), "ticket_no")
    .groupBy('book_ref, 'passenger_id)
    .agg(sum('ticket_count).alias("count"))
    .agg(max('count))


  //7. Вывести максимальное количество перелётов на пассажира

  val df7 = df5_1.join(
      tickets.select('ticket_no, 'passenger_id), "ticket_no")
    .groupBy('passenger_id)
    .agg(sum('ticket_count).alias("count"))
    .agg(max('count))

  //  8. Вывести контактную информацию по пассажиру(ам) (passenger_id, passenger_name,
  //  contact_data) и общие траты на билеты, для пассажира потратившему минимальное
  //  количество денег на перелеты

  val df8_1 = df5_1.join(
      tickets.select('ticket_no, 'passenger_id, 'passenger_name, 'contact_data), "ticket_no")
    .groupBy('passenger_id, 'passenger_name, 'contact_data)
    .agg(sum('amount).alias("sum"))

  val df8 = df8_1.filter('sum === df8_1.select(min('sum)).first()(0))
    .select(concat(concat('passenger_id, lit("|"),
      'passenger_name, lit("|"), 'contact_data)))

  //  9. Вывести контактную информацию по пассажиру(ам) (passenger_id, passenger_name,
  //  contact_data) и общее время в полётах, для пассажира, который провёл максимальное
  //  время в полётах

  flightsV.persist()

  val i = flightsV.select('scheduled_duration).first()(0)

  //10. Вывести город(а) с количеством аэропортов больше одного

  val df10 = airports.select('city)
    .groupBy('city)
    .agg(count('city).alias("count"))
    .filter('count > 1)


  //11. Вывести город(а), у которого самое меньшее количество городов прямого сообщения

  val df11_1 = routes.select('departure_city, 'arrival_city)
    .groupBy('departure_city)
    .agg(countDistinct('arrival_city).alias("count"))

  val df11 = df11_1.select('departure_city)
    .filter('count === df11_1.select(min('count)).first()(0))


  //  12. Вывести пары городов, у которых нет прямых сообщений исключив реверсные дубликаты
  val df12_1 = routes.select('departure_city, 'arrival_city).distinct()

  val df12 = df12_1.as("t1").join(df12_1.as("t2"),
      $"t1.departure_city" < $"t2.arrival_city")
    .select($"t1.departure_city", $"t2.arrival_city")
    .except(df12_1)

  //  13. Вывести города, до которых нельзя добраться без пересадок из Москвы?

  val df13 = df12_1.filter('arrival_city =!= "Москва")
    .select('arrival_city)
    .except(df12_1.filter('departure_city === "Москва")
      .select('arrival_city))
    .distinct()

  // 14. Вывести модель самолета, который выполнил больше всего рейсов

  val df14_1 = flightsV.select('aircraft_code)
    .filter('status === "Arrived")
    .groupBy('aircraft_code)
    .agg(count('aircraft_code).alias("count"))

  val aircraft_code = df14_1.select('aircraft_code)
    .filter('count === df14_1.select(max('count)).first()(0))

  val df14 = aircrafts.join(aircraft_code, "aircraft_code")
    .select('model)


  //  15. Вывести модель самолета, который перевез больше всего пассажиров
  val df15_1 = flightsV.select('aircraft_code, 'flight_id)
    .join(ticket_flights.select('flight_id), "flight_id")
    .groupBy('aircraft_code)
    .agg(count('aircraft_code).alias("count"))

  val aircraft_code_new = df15_1.select('aircraft_code)
    .filter('count === df15_1.select(max('count)).first()(0))

  val df15 = aircrafts.join(aircraft_code_new, "aircraft_code")
    .select('model)

  //  16. Вывести отклонение в минутах суммы запланированного времени перелета от
  //  фактического по всем перелётам

  //17. Вывести города, в которые осуществлялся перелёт из Санкт-Петербурга 2016-09-13

  val df17 = flightsV.select('arrival_city, 'actual_departure)
    .filter('departure_city === "Санкт-Петербург")
    .filter('status === "Arrived")
    .withColumn("departure_date", to_date(col("actual_departure"), "yyyy-MM-dd"))
    .filter('departure_date === "2016-09-13")
    .select('arrival_city)
    .distinct()

  //  18. Вывести перелёт(ы) с максимальной стоимостью всех билетов

  val df18_1 = ticket_flights.select('flight_id, 'amount)
    .groupBy('flight_id)
    .agg(sum('amount).alias("sum"))

  val df18 = df18_1.select('flight_id)
    .filter('sum === df18_1.select(max('sum)).first()(0))

//19. Выбрать дни в которых было осуществлено минимальное количество перелётов
val df19_1 = flightsV.select( 'actual_departure)
  .filter('status === "Arrived")
  .withColumn("departure_date", to_date(col("actual_departure"), "yyyy-MM-dd"))
  .groupBy('departure_date)
  .agg(count('departure_date).alias("count"))

  val df19 = df19_1.select('departure_date)
    .filter('count === df19_1.select(min('count)).first()(0))

//  20. Вывести среднее количество вылетов в день из Москвы за 09 месяц 2016 года

  val df20 = flightsV.select('actual_departure)
    .filter('status === "Arrived")
    .filter('departure_city === "Москва")
    .withColumn("departure_date", to_date(col("actual_departure"), "yyyy-MM-dd"))
    .filter('departure_date >= "2016-09-01")
    .filter('departure_date <= "2016-09-30")
    .select(count('departure_date) / 30)

//  21. Вывести топ 5 городов у которых среднее время перелета до пункта назначения больше 3  часов
}
