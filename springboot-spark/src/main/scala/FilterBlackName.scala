import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量
  */
object FilterBlackName {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FilterBlackName").setMaster("local")
    val sc = new SparkContext(conf)

    val nameRdd = sc.textFile("names")
    val blackNames = List("da", "xiao")
    val blackNameBroadcast = sc.broadcast(blackNames)
    nameRdd.filter(x => {
      ! blackNameBroadcast.value.contains(x)
    }).foreach(println)

    sc.stop()



  }

}
