import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Mapquest and Visual Crossing")
      .config("spark.master", "local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val weatherStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "weather")
      .load()

    val incidentStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "incident")
      .load()

    val weatherRawDF = weatherStream.selectExpr("CAST(value as STRING)")
    val incidentRawDF = incidentStream.selectExpr("CAST(value as STRING)")

    val weatherSchema = new StructType()
      .add("datetime", StringType)
      .add("datetimeepoch", StringType)
      .add("temp", StringType)
      .add("feelslike", StringType)
      .add("humidity", StringType)
      .add("dew", StringType)
      .add("precip", StringType)
      .add("precipprob", StringType)
      .add("snow", StringType)
      .add("snowdepth", StringType)
      .add("preciptype", StringType)
      .add("windgust", StringType)
      .add("windspeed", StringType)
      .add("winddir", StringType)
      .add("pressure", StringType)
      .add("visibility", StringType)
      .add("cloudcover", StringType)
      .add("solarradiation", StringType)
      .add("solarenergy", StringType)
      .add("uvindex", StringType)
      .add("conditions", StringType)
      .add("icon", StringType)
      .add("stations", StringType)
      .add("sunrise", StringType)
      .add("sunriseepoch", StringType)
      .add("sunset", StringType)
      .add("sunsetepoch", StringType)
      .add("moonphase", StringType)
      .add("latitude", StringType)
      .add("longitude", StringType)

    val incidentSchema = new StructType()
      .add("id", StringType)
      .add("type", StringType)
      .add("severity", StringType)
      .add("eventcode", StringType)
      .add("lat", StringType)
      .add("lng", StringType)
      .add("starttime", StringType)
      .add("endtime", StringType)
      .add("impacting", StringType)
      .add("shortdesc", StringType)
      .add("fulldesc", StringType)
      .add("delayfromfreeflow", StringType)
      .add("delayfromtypical", StringType)
      .add("distance", StringType)


    val weatherDF = weatherRawDF.select(from_json(col("value"), weatherSchema).as("data")).select("data.*")
    val incidentDF = incidentRawDF.select(from_json(col("value"), incidentSchema).as("data")).select("data.*")

//    val joinDF = incidentDF.join(weatherDF, incidentDF("lat") === weatherDF("latitude") && incidentDF("lng") === weatherDF("longitude"))
    val joinTemp = weatherDF
      .join(incidentDF, incidentDF("lat") === weatherDF("latitude") && incidentDF("lng") === weatherDF("longitude"), "inner")
//println(joinDF)
//    val incidentStreamDF = incidentDF
//      .writeStream
//      //      .trigger(Trigger.ProcessingTime("1 seconds"))
//      //      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
//      //        prStringln(s"Writing to Cassandra $batchID")
//      //        batchDF.write
//      //          .cassandraFormat("nb_pol", "galvin")
//      //          .mode("append")
//      //          .save()
//      //      }
//      .outputMode("append")
//      .format("console")
//      .start()
//
    val trafficWeatherStream = joinTemp
      .writeStream
            .trigger(Trigger.ProcessingTime("1 seconds"))
            .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
              println(s"Writing to Cassandra $batchID")
//              println(batchDF)
              batchDF.write
                .cassandraFormat("trafficweatherdata", "apache")
                .mode("append")
                .save()
            }
      .outputMode("append")
//      .format("console")
      .start()

    trafficWeatherStream.awaitTermination()
  }

}
