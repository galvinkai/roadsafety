
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming.toDStreamFunctions
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql
import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol

import scala.reflect.internal.util.TriState.True



object temp{
  case class model(id: Integer, eventcode: Integer, lat: Integer, lng: Integer, severity: Integer)
  def main(args: Array[String]): Unit = {

    val broker_id ="localhost:9092"
    val groupid ="GRP1"
    val topics="testtopic"
    val topicset=topics.split(",").toSet
    val kafkaParams=Map[String,Object] (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    /*     Integration with cassandra     */
    //val collection = sc.parallelize()
    val spark = SparkSession.builder()
      .appName("Cassandra Spark")
      .master("local[*]")
      .config("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("spark.sql.extensions","com.datastax.spark.connector.CassandraSparkExtensions")
      .getOrCreate()
    val v1 = spark.sparkContext.cassandraTable("apache", "traffic")
    v1.foreach(row => println(row.toString()) )
    spark.createDataFrame(Seq(model(4, 12, 1322, 5000, 2))).toDF("id", "eventcode", "lat", "lng", "severity").rdd.saveToCassandra("apache", "traffic", SomeColumns("id", "eventcode", "lat", "lng", "severity"))

    val sparkconf=new SparkConf().setMaster("local").setAppName("Kafka Demo")
    val ssc = new StreamingContext(sparkconf,Seconds(10))
    //Read parquet stream
    val df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", broker_id)
      .option("subscribe", "testtopic")
      .option("startingOffsets", "earliest")
      .load()
    )

    ssc.start()
    ssc.awaitTermination()

  }

}

