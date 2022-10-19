ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.5"

lazy val root = (project in file("."))
  .settings(
    name := "MapquestWeather"
  )

//spark-packages
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
// https://mvnrepository.com/artifact/org.apache.spark/spark-avro
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.8"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.8"
libraryDependencies +=  "org.apache.spark" % "spark-streaming_2.12" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.8"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5"
// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.2"
//libraryDependencies += "com.typesafe" % "config" % "1.3.2"
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.2"
libraryDependencies += "me.vican.jorge" %% "dijon" % "0.6.0"

