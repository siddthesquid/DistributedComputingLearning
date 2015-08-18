/**
 * Created by ssingal on 7/9/15.
 */

import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.Options
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.storm.option.{SpoutParallelism, FlatMapParallelism, SummerParallelism}
import com.twitter.summingbird.storm.{StormStore, Storm, Executor, StormExecutionConfig}
import backtype.storm.{Config => BTConfig}
import com.twitter.scalding.Args
import com.twitter.tormenta.scheme.Scheme
import com.twitter.tormenta.spout.KafkaSpout


object ExeStorm {
  def main(args: Array[String]) {
    Executor(args, StormRunner(_))
  }
}

/**
 * The following object contains code to execute the Summingbird
 * WordCount job defined in ExampleJob.scala on a storm
 * cluster.
 */
object StormRunner {
  /**
   * These imports bring the requisite serialization injections, the
   * time extractor and the batcher into implicit scope. This is
   * required for the dependency injection pattern used by the
   * Summingbird Storm platform.
   */
  import Serialization._, WordCountJob._, Config._, Redis._


  val scheme = new Scheme[String] {
    override def decode(bytes: Array[Byte]): TraversableOnce[String] =
      Some(new String(bytes))
  }

  /**
   * "spout" is a concrete Storm source for Status data. This will
   * act as the initial producer of Status instances in the
   * Summingbird word count job.
   */
  val spout = new KafkaSpout(
    scheme,
    KafkaZkConnect,
    "/brokers",
    KafkaTopic,
    AppName,
    "/consumers",
    -1
  )

  /**
   * And here's our MergeableStore supplier.
   *
   * A supplier is required (vs a bare store) because Storm
   * serializes every constructor parameter to its
   * "bolts". Serializing a live memcache store is a no-no, so the
   * Storm platform accepts a "supplier", essentially a function0
   * that when called will pop out a new instance of the required
   * store. This instance is cached when the bolt starts up and
   * starts merging tuples.
   *
   * A MergeableStore is a store that's aware of aggregation and
   * knows how to merge in new (K, V) pairs using a Monoid[V]. The
   * Monoid[Long] used by this particular store is being pulled in
   * from the Monoid companion object in Algebird. (Most trivial
   * Monoid instances will resolve this way.)
   *
   * First, the backing store:
   */
  lazy val viewCountStore =
    Redis.mergeable[(String, BatchID), Long]("wordCountStore")

  /**
   * the param to store is by name, so this is still not created created
   * yet
   */
  val storeSupplier: StormStore[String, Long] = Storm.store(viewCountStore)

  /**
   * This function will be called by the storm runner to request the info
   * of what to run. In local mode it will start up as a
   * separate thread on the local machine, pulling tweets off of the
   * TwitterSpout, generating and aggregating key-value pairs and
   * merging the incremental counts in the memcache store.
   *
   * Before running this code, make sure to start a local memcached
   * instance with "memcached". ("brew install memcached" will get
   * you all set up if you don't already have memcache installed
   * locally.)
   */

  def apply(args: Args): StormExecutionConfig = {
    new StormExecutionConfig {
      override val name = "SummingbirdExample"

      // No Ackers
      override def transformConfig(config: Map[String, AnyRef]): Map[String, AnyRef] = {
        config ++ List((BTConfig.TOPOLOGY_ACKER_EXECUTORS -> (new java.lang.Integer(0))))
      }

      override def getNamedOptions: Map[String, Options] = Map(
        "DEFAULT" -> Options().set(SummerParallelism(2))
          .set(FlatMapParallelism(80))
          .set(SpoutParallelism(2))
          .set(CacheSize(100))
      )
      override def graph = job[Storm](spout, storeSupplier)
    }
  }

  /**
   * Once you've got this running in the background, fire up another
   * repl and query memcached for some counts.
   *
   * The following commands will look up words. Hitting a word twice
   * will show that Storm is updating Memcache behind the scenes:
    {{{
    scala>     lookup("i") // Or any other common word
    res7: Option[Long] = Some(1774)
    scala>     lookup("i")
    res8: Option[Long] = Some(1779)
    }}}
   */
//  def lookup(lookId: Long): Option[Long] =
//    Await.result {
//      viewCountStore.get(lookId -> ViewCount.batcher.currentBatch)
//    }
}
