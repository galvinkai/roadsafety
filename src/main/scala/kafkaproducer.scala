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
    val cordURL = "http://www.mapquestapi.com/directions/v2/route?key=462wlx2emg9VA0jpVNS7rn76fY14G2Hq&from=WashingtonDC,USA&to=Newyork,USA"
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
    var j = 0
    while (i <= 20)  {
      try {
        val incidentsUrl = "https://www.mapquestapi.com/traffic/v2/incidents?key=462wlx2emg9VA0jpVNS7rn76fY14G2Hq&filters=congestion&boundingBox=" + lowLat + "," + lowLng + "," + upLat + "," + upLng //39.744431,-75.141426,39.958858,-75.55426"   //39.95,-105.25,39.52,-104.71"
        val incidentsHttpClient = HttpClientBuilder.create().build()
        val incidentsHttpResponse = incidentsHttpClient.execute(new HttpGet(incidentsUrl))
        val incidentsEntity = incidentsHttpResponse.getEntity
        val incidentsStr = EntityUtils.toString(incidentsEntity, "UTF-8")
        val incidentsJson = parse(incidentsStr).incidents
        val incidentsLong = incidentsJson(j).lng
        val incidentsLat = incidentsJson(j).lat
        val incidentsJsonStr = incidentsJson(j).toString()

        val weatherUrl = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/" + incidentsLat + "%2C%20" + incidentsLong + "/today?unitGroup=metric&include=current%2Cevents%2Calerts&key=NJ5ANF65ZUXHMSZBMQ9NVJZ3H&contentType=json"
        val weatherHttpClient = HttpClientBuilder.create().build()
        val weatherHttpResponse = weatherHttpClient.execute(new HttpGet(weatherUrl))
        val weatherEntity = weatherHttpResponse.getEntity
        val weatherStr = EntityUtils.toString(weatherEntity, "UTF-8")
        val weatherJson = parse(weatherStr).currentConditions
        weatherJson.latitude = parse(weatherStr).latitude.toString()
        weatherJson.longitude = parse(weatherStr).longitude.toString()
        val weatherJsonStr = weatherJson.toString()

        println("============================== INCIDENT DATA ====================================")
        val incidentData = new ProducerRecord[String, String]("incident", null, incidentsJsonStr)
        println("ilt: ", incidentsLat)
        println("ilg", incidentsLong)
        println(incidentsUrl)
        println(incidentData)
        producer.send(incidentData)

        println("================================================================================")
        println("============================== WEATHER DATA ====================================")
        val weatherData = new ProducerRecord[String, String]("weather", null, weatherJsonStr)
        println("wlt: ", parse(weatherStr).latitude)
        println("wlg: ", parse(weatherStr).longitude)
        println(weatherData)
        println("================================================================================")
        producer.send(weatherData)
        sleep(10000)
        j = j + 1
        producer.flush()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}


