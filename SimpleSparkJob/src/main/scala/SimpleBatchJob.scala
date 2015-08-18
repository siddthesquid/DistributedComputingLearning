import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by ssingal on 8/6/15.
 */
object ExecuteBatchJob {
  def main (args: Array[String]) {
    val s:SimpleBatchJob = new A
  }
}


abstract class SimpleBatchJob {




  val sc = defineSparkContext()
  val im = new InputManager(sc)

  def defineSparkContext():SparkContext
  def defineInputs(inputs:Map[String,SparkInput])
  def defineOutputs(outputs:Map[String,SparkOutput])
  def defineJobs(jobs:Map[String,RDD=>RDD])
  def defineTopology()

  def execute (): Unit ={

  }


}

class A extends SimpleBatchJob
