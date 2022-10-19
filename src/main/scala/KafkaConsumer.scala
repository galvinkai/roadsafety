import org.apache.spark.sql._

case class WeatherDF (
                       cloudcover: String,
                       conditions: String,
                       datetime: String,
                       datetimeEpoch: String,
                       dew: String,
                       feelslike: String,
                       humidity: String,
                       icon: String,
                       moonphase: String,
                       precip: String,
                       precipprob: String,
                       preciptype: String,
                       pressure: String,
                       snow: String,
                       snowdepth: String,
                       solarenergy: String,
                       solarradiation: String,
                       stations: String,
                       sunrise: String,
                       sunriseEpoch: String,
                       sunset: String,
                       sunsetEpoch: String,
                       temp: String,
                       uvindex: String,
                       visibility: String,
                       winddir: String,
                       windgust: String,
                       windspeed: String)

//case class IncidentDF(delayedFromFreeFlow: String, delayedFromTypical: String, distance: String, endTime: String, eventCode: String, fullDesc: String, iconURL: String, id: String, impacting: Boolean, lat: String, lng: String, parameterizeDescription: String, severity: String, shortDesc: String, startTime: String, type': String)

object KafkaConsumer {
  //  case class model(id: Stringeger, eventcode: Stringeger, lat: Stringeger, lng: Stringeger, severity: Stringeger)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Mapquest and Visual Crossing")
      .config("spark.master", "local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()


    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      //          .option("startingOffsets", """{"incident":{"0":23,"1":-2},"weather":{"0":-2}}""")
      //          .option("endingOffsets", """{"incident":{"0":50,"1":-1},"weather":{"0":-1}}""")
      .option("subscribe", "incident, weather")
      .load()


    val rawDF = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]

    //    df.selectExpr("CAST(value AS STRING)", "headers").as[(String, String, Array[(String, Array[Byte])])]

    //        val streamDF = rawDF.map(row => row.split(":"))
    //          .map(row => WeatherDF(
    //            row(0),
    //            row(1),
    //            row(2),
    //            row(3),
    //            row(4),
    //            row(5),
    //            row(6),
    //            row(7),
    //            row(8),
    //            row(9),
    //            row(10),
    //            row(11),
    //            row(12),
    //            row(13),
    //            row(14),
    //            row(15),
    //            row(16),
    //            row(17),
    //            row(18),
    //            row(19),
    //            row(20),
    //            row(21),
    //            row(22),
    //            row(23),
    //            row(24),
    //            row(25),
    //            row(26),
    //            row(27)
    //          ))

    //        val String_df = streamDF.select("String")



    val weatherStream = rawDF
      .writeStream
      //      .trigger(Trigger.ProcessingTime("1 seconds"))
      //      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
      //        prStringln(s"Writing to Cassandra $batchID")
      //        batchDF.write
      //          .cassandraFormat("nb_pol", "galvin")
      //          .mode("append")
      //          .save()
      //      }
      .outputMode("update")
      .format("console")
      .start()

    //    val query = String_df.writeStream.outputMode("update").format("console").start()

    weatherStream.awaitTermination()


    //      df1.rdd.saveToCassandra("apache", "trafficcongestion",
    //      SomeColumns("id", "type", "severity", "eventcode", "lat",
    //        "lng", "starttime", "endtime", "impacting", "shortdesc", "fulldesc",
    //        "delayfromfreeflow", "delayfromtypical", "distance"))


    //    incidentDF.join(weather, empDF("lat") === weatherDF("dept_id") && Leaddetails("Utm_Source") === Utm_Master("Utm_Source"), "inner")
    //      .join(addDF, empDF("emp_id") === addDF("emp_id"), "inner")
    //      .show(false)
    //


  }

}
