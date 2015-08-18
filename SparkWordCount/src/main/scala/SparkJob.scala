import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ssingal on 7/14/15.
 */
object SparkJob {

  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkWordCount")
    val sc = new SparkContext(conf)

    val input = sc.textFile("/home/ssingal/Documents/testFile.txt")

    val counts = input.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    counts.saveAsTextFile("counts.txt")

  }

}
