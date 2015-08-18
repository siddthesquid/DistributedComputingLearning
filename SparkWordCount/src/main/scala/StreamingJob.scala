/**
 * Created by ssingal on 7/13/15.
 */


import kafka.serializer.StringDecoder
import Config._
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka._
import com.redis._

object KafkaWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(AppName)
    val ssc = new StreamingContext(conf, org.apache.spark.streaming.Seconds(2))
    ssc.checkpoint("checkpoint")

    //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val topics = Set(KafkaTopic)
    val kafkaParams = Map(
    "metadata.broker.list" -> KafkaBrokerConnect,
    "auto.offset.reset" -> "smallest"
    )
    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics).map(_._2)
    val words = lines.flatMap(_.split(" ")).map(_.trim)
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKey(_ + _)

//    wordCounts.foreachRDD(rdd => rdd.foreach( { x =>
//        val (word, count) = x
//        r.incrby(word,count.toInt)
//    }))

//    wordCounts.foreachRDD({rdd =>
//      val r = new RedisClient("localhost",6379)
//      rdd.mapPartitions({x=>
//        val (word,count) = x
//        r.incrby(word,count)
//        r.get(word)
//      }
//    )})

    wordCounts.foreachRDD({rdd =>
      rdd.foreachPartition({map =>
        val red = new RedisClient("localhost",6379)
        map.foreach({ elem =>
          val (word, count) = elem
          red.incrby(word,count.toInt)
        })
      })
    })

    wordCounts.print()

    wordCounts.saveAsTextFiles("testWordCount","txt")

    ssc.start()
    ssc.awaitTermination()
  }
}