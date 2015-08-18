import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.{TimeExtractor, Producer, Platform, TailProducer}

/**
 * Created by ssingal on 7/8/15.
 */
object WordCountJob {

  implicit val timeOf: TimeExtractor[String] = TimeExtractor(_=> 5L)
  //implicit val batcher = Batcher.ofHours(1)
  implicit val batcher = Batcher.ofMinutes(5)

  def tokenize(text: String): TraversableOnce[String] =
    text.toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")

  def job[P <: Platform[P]]
    (line: Producer[P,String],
     counts: P#Store[String, Long]) =
    line
      .flatMap {tokenize(_)}
      .flatMap {word: String => Seq((word -> 1L)) }
      .sumByKey(counts)

}
