/**
 * Created by ssingal on 8/6/15.
 */
object WordCount extends SimpleBatchJob {

  def main(args: Array[String]): Unit = {
    val textInput = textFile()
    val redisInput = redis()
    val output1 = tsv
    val output2 = tsv
  }



}
