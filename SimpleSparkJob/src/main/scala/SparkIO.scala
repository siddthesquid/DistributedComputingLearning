import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
 * Created by ssingal on 8/6/15.
 */
abstract class SparkInput(sc:SparkContext) {

  def getRDD()

}

object AdditionalSparkIO {

  implicit class AdditionalInputs(sc:SparkContext){

    def redisHashSet(host:String,port:Int) ={
      val redisInput = new Jedis(host,port)
      val keys = redisInput.hmget("*")
    }


  }

  implicit class AdditionalOutputs(rdd:RDD){

  }

}