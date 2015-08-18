import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

import java.util.Properties

import kafka.serializer.StringEncoder
import org.slf4j.LoggerFactory

/**
 * Created by ssingal on 7/8/15.
 */
object RandomLineStreamerRunner {
  def main(args:Array[String]): Unit ={
    RandomLineStreamer.run
  }
}


object RandomLineStreamer {

  import Config._
  private val logger = LoggerFactory.getLogger(this.getClass)

  val possibleLines: Seq[String] = Seq(
    "this is a random sentence with some random words in it so i can test this program out",
    "one of these lines will be chosen to enter the kafka stream",
    "the chosen line will then be tokenized and mapped to the value of one",
    "the tokenized and mapped words will then move into the store",
    "and a sum by key will be performed",
    "i am going to write another sentence just to make the size of this list larger",
    "this is a random sentence with some random words in it so i can test this program out",
    "b b",
    "c c c",
    "d d d d",
    "e e e e e",
    "f f f f f f"
  )

  def run: Unit = {
    val props = new Properties();

    props.put("metadata.broker.list", KafkaBrokerConnect);
    props.put("key.serializer.class", classOf[StringEncoder].getName);
    //props.put("request.required.acks", "1");

    val config = new ProducerConfig(props);

    val producer = new Producer[java.lang.String,Array[Byte]](new ProducerConfig(props))

    while(true) {
      val randomLine: String = possibleLines(scala.util.Random.nextInt(possibleLines.length))
      logger.info(s"sending $randomLine")
      producer.send(new KeyedMessage(KafkaTopic,randomLine.hashCode.toString,randomLine.getBytes))
      Thread.sleep(1000);
    }
  }

}
