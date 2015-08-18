/**
 * Created by ssingal on 7/9/15.
 */
object Config {


  val AppName = "wordcount"

  val JobDir = "/tmp/wordcount/"
  val DataDir = JobDir + "data/"

  val RedisConnect = "localhost:6379"
  val KafkaZkConnect = "localhost:2181"
  val KafkaBrokerConnect = "localhost:9092"
  val KafkaTopic = "wordcountqueue"


}
