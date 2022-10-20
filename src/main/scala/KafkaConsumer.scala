import com.datastax.spark.connector.cql.Schema
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_json, lit}
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

    import spark.implicits._

    val weatherStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "weather")
      .load()

    val weatherStreamLat = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "weatherAll")
      .load()

    val weatherStreamLong = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "weatherLong")
      .load()

    val incidentStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "incidents")
      .load()

    val weatherRawDF = weatherStream.selectExpr("CAST(value as STRING)")
    val weatherRawLatDF = weatherStreamLat.selectExpr("CAST(value as STRING)")
    val weatherRawLongDF = weatherStreamLong.selectExpr("CAST(value as STRING)")
    val incidentRawDF = incidentStream.selectExpr("CAST(value as STRING)")

    val weatherSchema = new StructType()
      .add("latitude", StringType)
      .add("longitude", StringType)
      .add("datetime", StringType)
      .add("datetimeEpoch", StringType)
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
      .add("sunriseEpoch", StringType)
      .add("sunset", StringType)
      .add("sunsetEpoch", StringType)
      .add("moonphase", StringType)

    val incidentSchema = new StructType()
      .add("id", StringType)
      .add("type", StringType)
      .add("severity", StringType)
      .add("eventCode", StringType)
      .add("lat", StringType)
      .add("lng", StringType)
      .add("startTime", StringType)
      .add("endTime", StringType)
      .add("impacting", StringType)
      .add("shortDesc", StringType)
      .add("fullDesc", StringType)
      .add("delayFromFreeFlow", StringType)
      .add("delayFromTypical", StringType)
      .add("distance", StringType)

    val locationSchema = new StructType()
      .add("value", StringType)

    val weatherLatDF = weatherRawLatDF.select(from_json(col("value"), weatherSchema)
      .as("data"))
      .select("data.*")
    val weatherLongDF = weatherRawLongDF.select(from_json(col("value"), weatherSchema)
      .as("data"))
      .select("data.*")
//    println("=======================================================")
//    println(weatherLatDF)
//    println("=======================================================")

    val weatherDF = weatherRawDF.select(from_json(col("value"), weatherSchema)
      .as("data"))
      .select("data.*")
      .unionByName(weatherLatDF, true)
//      .withColumn("latitude", lit(weatherLatDF))
//      .withColumn("longitude", lit(weatherLongDF))

    val incidentDF = incidentRawDF.select(from_json(col("value"), incidentSchema).as("data")).select("data.*")


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
    val weatherStreamDF = weatherLatDF
      .writeStream
      //      .trigger(Trigger.ProcessingTime("1 seconds"))
      //      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
      //        prStringln(s"Writing to Cassandra $batchID")
      //        batchDF.write
      //          .cassandraFormat("nb_pol", "galvin")
      //          .mode("append")
      //          .save()
      //      }
      .outputMode("append")
      .format("console")
      .start()



    //    val query = String_df.writeStream.outputMode("update").format("console").start()
//    incidentStreamDF.awaitTermination()
    weatherStreamDF.awaitTermination()



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
