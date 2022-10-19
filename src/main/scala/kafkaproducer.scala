import java.util.Properties
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.utils.Utils.sleep
import dijon._

object kafkaproducer {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafka Producer 1")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

//API call to get the coordinates of a location from and location to
    val cordURL = "http://www.mapquestapi.com/directions/v2/route?key=GO6Xati6oXWr2xPJ3tzx0U941GmKdFvB&from=WashingtonDC,USA&to=Newyork,USA"
    val httpClient = HttpClientBuilder.create().build()
    val httpResponse = httpClient.execute(new HttpGet(cordURL))
    val entity = httpResponse.getEntity
    val str = EntityUtils.toString(entity, "UTF-8")
    val json = parse(str).route.boundingBox
    val lowLng = json.lr.lng
    val lowLat = json.lr.lat
    val upLng = json.ul.lng
    val upLat = json.ul.lat

//API call using the coordinates above to get the traffic incidents
    val i = 1
    while (i <= 20)  {
      try {
        val incidentsUrl = "https://www.mapquestapi.com/traffic/v2/incidents?key=GO6Xati6oXWr2xPJ3tzx0U941GmKdFvB&filters=congestion&boundingBox=" + lowLat + "," + lowLng + "," + upLat + "," + upLng //39.744431,-75.141426,39.958858,-75.55426"   //39.95,-105.25,39.52,-104.71"
        val incidentsHttpClient = HttpClientBuilder.create().build()
        val incidentsHttpResponse = incidentsHttpClient.execute(new HttpGet(incidentsUrl))
        val incidentsEntity = incidentsHttpResponse.getEntity
        val incidentsStr = EntityUtils.toString(incidentsEntity, "UTF-8")
        val incidentsJson = parse(incidentsStr).incidents
        val incidentsLong = incidentsJson(0).lng
        val incidentsLat = incidentsJson(0).lat
        val incidentsJsonStr = incidentsJson.toString()

        val weatherUrl = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"+incidentsLat+"%2C%20"+incidentsLong+"/today?unitGroup=metric&include=current%2Cevents%2Calerts&key=NJ5ANF65ZUXHMSZBMQ9NVJZ3H&contentType=json"
        val weatherHttpClient = HttpClientBuilder.create().build()
        val weatherHttpResponse = weatherHttpClient.execute(new HttpGet(weatherUrl))
        val weatherEntity = weatherHttpResponse.getEntity
        val weatherStr = EntityUtils.toString(weatherEntity, "UTF-8")
        val weatherJson = parse(weatherStr).currentConditions
        val weatherJsonStr = weatherJson.toString()

        println("============================== INCIDENT DATA ====================================")
        val incidentData = new ProducerRecord[String, String]("incident", "key", incidentsJsonStr)
//        println(incidentsLat)
//        println(incidentsLong)
          println(incidentData)
        producer.send(incidentData)
        println("================================================================================")
        println("============================== WEATHER DATA ====================================")
        val weatherData = new ProducerRecord[String, String]("weather","key", weatherJsonStr)
//        println(weatherJson.latitude)
//        println(weatherJson.longitude)
          println(weatherData)
        println("================================================================================")
        producer.send(weatherData)
        sleep(10000)
        producer.flush()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}


