/**
 *
 * Created by ssingal on 7/8/15.
 */
import storm.kafka.{SpoutConfig, KafkaSpout}
import com.twitter.tormenta.spout.Spout
class RichKafkaSpout(zkConnectionString: String, topic: String, appID: String, nThreads: Int = 1)(fn: Array[Byte] => TraversableOnce[T]) extends KafkaSpout(zkConnectionString, topic, appID, nThreads)(fn) with Spout{
  override def flatMap[U](fn: (T) => TraversableOnce[U]): Spout[U] = {
    new RichKafkaSpout(zkConnectionString, topic, appID, nThreads)(fn(_).flatMap(newFn))
  }
}
