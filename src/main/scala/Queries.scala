import Service._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import spark.implicits._


object Queries extends App {


  val df = tickets
    .groupBy('book_ref)
    .agg(count('book_ref) as "count")

  df.persist()

  //  1. Вывести максимальное количество человек в одном бронировании
  val df1 = df.agg(max('count) as "result")
    .select(lit(1) as "id", 'result)

  //  2. Вывести количество бронирований с количеством людей больше среднего значения
  //    людей на одно бронирование

  val df2 = df.filter('count >= df.select(avg('count)).first()(0))
    .select(lit(1) as "id", count('book_ref) as "result")
    .orderBy('result)

  /*3. Вывести количество бронирований, у которых состав пассажиров повторялся два и более
  раза, среди бронирований с максимальным количеством людей (п.1)?*/
  val windowSpec = Window.partitionBy("passenger_id")

  val df3_1 = df.filter('count === df1.first()(1)).select('book_ref)

  val df3_2 = tickets
    .select('book_ref, 'passenger_id)
    .withColumn("count", count(col("passenger_id")).over(windowSpec))
    .select('book_ref, 'passenger_id).filter('count > 1)

  val df3 = df3_2.join(df3_1, "book_ref")
    .groupBy('book_ref)
    .agg(sort_array(collect_list('passenger_id)) as "passengerList")
    .groupBy('passengerList)
    .agg(count('passengerList).alias("count"))
    .filter('count > 1)
    .agg(count('passengerList) as "result")
    .select(lit(3) as "id", 'result)


  //4. Вывести номера брони и контактную информацию по пассажирам в брони (passenger_id,
  //  passenger_name, contact_data) с количеством людей в брони = 3

  val df4_1 = df.filter('count === 3).select('book_ref)

  val df4 = tickets
    .select('book_ref, concat('passenger_id, lit("|"),
      'passenger_name, lit("|"), 'contact_data) as "cd")
    .join(df4_1, "book_ref")
    .groupBy('book_ref)
    .agg(collect_list('cd) cast "string" as "list")
    .select(lit(4) as "id", concat('book_ref, lit("|"), 'list) as "result")
    .orderBy('result)

  df.unpersist()

  //  5. Вывести максимальное количество перелётов на бронь


  val df5_1 = ticket_flights.select('flight_id, 'ticket_no, 'amount)
    .join(flightsV.filter('status === "Arrived")
      .select('flight_id), "flight_id")
    .groupBy('ticket_no)
    .agg(count('ticket_no) as "ticket_count", sum('amount) as "amount")

  df5_1.persist()

  val df5 = df5_1.join(
      tickets.select('book_ref, 'ticket_no), "ticket_no")
    .groupBy('book_ref)
    .agg(sum('ticket_count) as "count")
    .agg(max('count) as "result")
    .select(lit(5) as "id", 'result)

  // 6. Вывести максимальное количество перелётов на пассажира в одной брони

  val df6 = df5_1.join(
      tickets.select('book_ref, 'ticket_no, 'passenger_id), "ticket_no")
    .groupBy('book_ref, 'passenger_id)
    .agg(sum('ticket_count) as "count")
    .agg(max('count) as "result")
    .select(lit(6) as "id", 'result)


  //7. Вывести максимальное количество перелётов на пассажира

  val df7 = df5_1.join(
      tickets.select('ticket_no, 'passenger_id), "ticket_no")
    .groupBy('passenger_id)
    .agg(sum('ticket_count) as "count")
    .agg(max('count) as "result")
    .select(lit(7) as "id", 'result)

  //  8. Вывести контактную информацию по пассажиру(ам) (passenger_id, passenger_name,
  //  contact_data) и общие траты на билеты, для пассажира потратившему минимальное
  //  количество денег на перелеты

  val df8_1 = df5_1.join(
      tickets.select('ticket_no, 'passenger_id, 'passenger_name, 'contact_data), "ticket_no")
    .groupBy('passenger_id, 'passenger_name, 'contact_data)
    .agg(sum('amount) as "sum")

  val df8 = df8_1.filter('sum === df8_1.select(min('sum)).first()(0))
    .select(concat('passenger_id, lit("|"),
      'passenger_name, lit("|"), 'contact_data, lit("|"), 'sum) as "result")
    .select(lit(8) as "id", 'result)
    .orderBy('result)

  df5_1.unpersist()

  //  9. Вывести контактную информацию по пассажиру(ам) (passenger_id, passenger_name,
  //  contact_data) и общее время в полётах, для пассажира, который провёл максимальное
  //  время в полётах


  val df9_1 = flightsV.filter('actual_duration.isNotNull)
    .select('flight_id, toDurationUDF('actual_duration) as "duration")
    .join(ticket_flights.select('flight_id, 'ticket_no), "flight_id")
    .join(tickets.select('ticket_no, 'passenger_id,
      'passenger_name, 'contact_data), "ticket_no")
    .groupBy('passenger_id, 'passenger_name, 'contact_data)
    .agg(sum('duration) as "sum")

  val df9 = df9_1.select(concat('passenger_id, lit("|"),
      'passenger_name, lit("|"), 'contact_data, lit("|"), 'sum) as "result")
    .filter('sum === df9_1.select(max('sum)).first()(0))
    .select(lit(9) as "id", 'result)
    .orderBy('result)


  //10. Вывести город(а) с количеством аэропортов больше одного

  val df10 = airports.select(lit(10) as "id", 'city)
    .groupBy('city)
    .agg(count('city) as "count")
    .filter('count > 1)
    .select(lit(10) as "id", 'city as "result")
    .orderBy('result)


  //11. Вывести город(а), у которого самое меньшее количество городов прямого сообщения

  val df11_1 = routes.select('departure_city, 'arrival_city)
    .groupBy('departure_city)
    .agg(countDistinct('arrival_city) as "count")

  val df11 = df11_1.select('departure_city as "result")
    .filter('count === df11_1.select(min('count)).first()(0))
    .select(lit(11) as "id", 'result)
    .orderBy('result)


  //  12. Вывести пары городов, у которых нет прямых сообщений исключив реверсные дубликаты
  val df12_1 = routes.select('departure_city, 'arrival_city).distinct()

  val df12 = df12_1.as("t1").join(df12_1.as("t2"),
      $"t1.departure_city" < $"t2.arrival_city")
    .select($"t1.departure_city" as "dc", $"t2.arrival_city" as "ac")
    .except(df12_1)
    .select(lit(12), concat('dc, lit("|"), 'ac) as "result")
    .orderBy('result)

  //  13. Вывести города, до которых нельзя добраться без пересадок из Москвы?

  val df13 = df12_1.filter('arrival_city =!= "Москва")
    .select('arrival_city as "result")
    .except(df12_1.filter('departure_city === "Москва")
      .select('arrival_city))
    .distinct()
    .select(lit(13) as "id", 'result)
    .orderBy('result)

  // 14. Вывести модель самолета, который выполнил больше всего рейсов

  val df14_1 = flightsV.select('aircraft_code)
    .filter('status === "Arrived")
    .groupBy('aircraft_code)
    .agg(count('aircraft_code) as "count")

  val aircraft_code = df14_1.select('aircraft_code)
    .filter('count === df14_1.select(max('count)).first()(0))

  val df14 = aircrafts.join(aircraft_code, "aircraft_code")
    .select(lit(14) as "id", 'model as "result")
    .orderBy('result)


  //  15. Вывести модель самолета, который перевез больше всего пассажиров
  val df15_1 = flightsV.select('aircraft_code, 'flight_id)
    .join(ticket_flights.select('flight_id), "flight_id")
    .groupBy('aircraft_code)
    .agg(count('aircraft_code) as "count")

  val aircraft_code_new = df15_1.select('aircraft_code)
    .filter('count === df15_1.select(max('count)).first()(0))

  val df15 = aircrafts.join(aircraft_code_new, "aircraft_code")
    .select(lit(15) as "id", 'model as "result")
    .orderBy('result)

  //  16. Вывести отклонение в минутах суммы запланированного времени перелета от
  //  фактического по всем перелётам

  val df16 = flightsV.filter('status === "Arrived")
    .select(
      (toDurationUDF('scheduled_duration) - toDurationUDF('actual_duration)) / 60 as "diff")
    .agg(sum('diff) as "result")
    .select(lit(16) as "id", 'result)

  //17. Вывести города, в которые осуществлялся перелёт из Санкт-Петербурга 2016-09-13

  val df17 = flightsV.select('arrival_city, 'actual_departure)
    .filter('departure_city === "Санкт-Петербург")
    .filter('status === "Arrived")
    .withColumn("departure_date", to_date(col("actual_departure"), "yyyy-MM-dd"))
    .filter('departure_date === "2016-09-13")
    .select(lit(17) as "id", 'arrival_city as "result")
    .distinct()
    .orderBy('result)

  //  18. Вывести перелёт(ы) с максимальной стоимостью всех билетов

  val df18_1 = ticket_flights.select('flight_id, 'amount)
    .groupBy('flight_id)
    .agg(sum('amount) as "sum")

  val df18 = df18_1.select(lit(18) as "id", 'flight_id as "result")
    .filter('sum === df18_1.select(max('sum)).first()(0))
    .orderBy('result)

  //19. Выбрать дни в которых было осуществлено минимальное количество перелётов
  val df19_1 = flightsV.select('actual_departure)
    .filter('status === "Arrived")
    .withColumn("departure_date", to_date(col("actual_departure"), "yyyy-MM-dd"))
    .groupBy('departure_date)
    .agg(count('departure_date) as "count")

  val df19 = df19_1.select(lit(19) as "id", 'departure_date as "result")
    .filter('count === df19_1.select(min('count)).first()(0))
    .orderBy('result)

  //  20. Вывести среднее количество вылетов в день из Москвы за 09 месяц 2016 года

  val df20 = flightsV.select('actual_departure)
    .filter('status === "Arrived")
    .filter('departure_city === "Москва")
    .withColumn("departure_date", to_date(col("actual_departure"), "yyyy-MM-dd"))
    .filter('departure_date >= "2016-09-01")
    .filter('departure_date <= "2016-09-30")
    .select(lit(20) as "id", count('departure_date) / 30 as "result")

  //  21. Вывести топ 5 городов у которых среднее время перелета до пункта назначения больше 3 часов

  val df21 = flightsV
    .filter('actual_duration.isNotNull).
    select('departure_city, toDurationUDF('actual_duration).alias("duration"))
    .groupBy('departure_city)
    .agg(avg('duration) as "avg")
    .filter('avg > 3 * 60 * 60)
    .orderBy('avg.desc)
    .limit(5)
    .select(lit(21) as "id", 'departure_city as "result")
    .orderBy('result)


  val results = df1.select('id, 'result)
    .union(df2)
    .union(df3)
    .union(df4)
    .union(df5)
    .union(df6)
    .union(df7)
    .union(df8)
    .union(df9)
    .union(df10)
    .union(df11)
    .union(df12)
    .union(df13)
    .union(df14)
    .union(df15)
    .union(df16)
    .union(df17)
    .union(df18)
    .union(df19)
    .union(df20)
    .union(df21)

  results
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .saveAsTable("school_de_stg.results_aamarkova")

}
