import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.cassandra._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//
//case class WeatherDF (
//cloudcover: String,
//conditions: String,
//datetime: String,
//datetimeEpoch: String,
//dew: String,
//feelslike: String,
//humidity: String,
//icon: String,
//moonphase: String,
//precip: String,
//precipprob: String,
//preciptype: String,
//pressure: String,
//snow: String,
//snowdepth: String,
//solarenergy: String,
//solarradiation: String,
//stations: String,
//sunrise: String,
//sunriseEpoch: String,
//sunset: String,
//sunsetEpoch: String,
//temp: String,
//uvindex: String,
//visibility: String,
//winddir: String,
//windgust: String,
//windspeed: String)

//case class IncidentDF(delayedFromFreeFlow: String, delayedFromTypical: String, distance: String, endTime: String, eventCode: String, fullDesc: String, iconURL: String, id: String, impacting: Boolean, lat: String, lng: String, parameterizeDescription: String, severity: String, shortDesc: String, startTime: String, type': String)

object KafkaConsumer {
  //  case class model(id: Stringeger, eventcode: Stringeger, lat: Stringeger, lng: Stringeger, severity: Stringeger)

  def main(args: Array[String]): Unit = {

    //    val spark = SparkSession
    //      .builder
    //      .appName("Mapquest and Visual Crossing")
    //      .config("spark.master", "local[*]")
    //      .config("spark.cassandra.connection.host", "127.0.0.1")
    //      .config("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog")
    //      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    //      .getOrCreate()
    //
    //    import spark.implicits._
    //
    //    val df = spark
    //      .readStream
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", "localhost:9092")
    //      .option("subscribe", "weather")
    //      .load()
    //
    //    val rawDF = df.selectExpr("CAST(value AS STRING)").as[String]
    //
    //    val streamDF = rawDF.map(row => row.split(":"))
    //      .map(row => WeatherDF(
    //        row(0),
    //        row(1),
    //        row(2),
    //        row(3),
    //        row(4),
    //        row(5),
    //        row(6),
    //        row(7),
    //        row(8),
    //        row(9),
    //        row(10),
    //        row(11),
    //        row(12),
    //        row(13),
    //        row(14),
    //        row(15),
    //        row(16),
    //        row(17),
    //        row(18),
    //        row(19),
    //        row(20),
    //        row(21),
    //        row(22),
    //        row(23),
    //        row(24),
    //        row(25),
    //        row(26),
    //        row(27)
    //      ))

    //    val String_df = streamDF.select("String")


    //    val makeUUID = udf(() => Uuids.timeBased().toString)

    //    val query = sentiment_stream.withColumn("uuid", makeUUID())
    //      .withColumnRenamed("prediction", "result")

    //    val weatherStream = streamDF
    //      .writeStream
    ////      .trigger(Trigger.ProcessingTime("1 seconds"))
    ////      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
    ////        prStringln(s"Writing to Cassandra $batchID")
    ////        batchDF.write
    ////          .cassandraFormat("nb_pol", "galvin")
    ////          .mode("append")
    ////          .save()
    ////      }
    //      .outputMode("update")
    //      .format("console")
    //      .start()

    //    val query = String_df.writeStream.outputMode("update").format("console").start()

    //    weatherStream.awaitTermination()

    val broker_id = "localhost:9092"
    val groupid = "GRP1"
    val weatherTopic = "weather"
    val incidentTopic = "incident"
    //    val mulitTopics = Array("source_1", "source_2", "source_3")
    val weather = weatherTopic.split(",").toSet
    val incident = incidentTopic.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val sparkconf = new SparkConf().setMaster("local").setAppName("Kafka Demo")
    val ssc = new StreamingContext(sparkconf, Seconds(10))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")

    val weatherData = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](weather, kafkaParams)
    )

    val incidentData = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](incident, kafkaParams)
    )

    /*  Stringegration with cassandra */
    val spark = SparkSession.builder()
      .appName("Cassandra Spark")
      .master("local[*]")
      .config("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .getOrCreate()
    //    data.map(record=> record.value()).prString()

//    val weatherdf = topic goes here and create data
//    val incidenntdf = topic goes here and create data


    weatherData.foreachRDD(rawRdd => {
      val rdd = rawRdd.map(_.value)
      val df = spark.read.json(rdd)
      println(df.show(5))

  /* Each record is moved to cassandra*/
    df.rdd.saveToCassandra("apache", "weathercurrent", SomeColumns(
      "cloudcover",
      "conditions",
      "datetime",
      "datetimeepoch",
      "dew",
      "feelslike",
      "humidity",
      "icon",
      "moonphase",
      "precip",
      "precipprob",
      "preciptype",
      "pressure",
      "snow",
      "snowdepth",
      "solarenergy",
      "solarradiation",
      "stations",
      "sunrise",
      "sunriseepoch",
      "sunset",
      "sunsetepoch",
      "temp",
      "uvindex",
      "visibility",
      "winddir",
      "windgust",
      "windspeed"))
    })

  incidentData.foreachRDD(rawIncident=> {
      val rdd1 = rawIncident.map(_.value.toString)
      val df1 = spark.read.json(rdd1)
    println(df1.show(5))

      df1.rdd.saveToCassandra("apache", "trafficcongestion",
      SomeColumns("id", "type", "severity", "eventcode", "lat",
        "lng", "starttime", "endtime", "impacting", "shortdesc", "fulldesc",
        "delayfromfreeflow", "delayfromtypical", "distance"))
    })

//    incidentDF.join(weather, empDF("lat") === weatherDF("dept_id") && Leaddetails("Utm_Source") === Utm_Master("Utm_Source"), "inner")
//      .join(addDF, empDF("emp_id") === addDF("emp_id"), "inner")
//      .show(false)
//
    ssc.start()
    ssc.awaitTermination()

  }

}
