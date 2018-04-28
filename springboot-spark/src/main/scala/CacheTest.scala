import org.apache.spark.{SparkConf, SparkContext}

object CacheTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CacheTest").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      * cache算子使用的注意事项：
      *  1、cache算子是一个懒执行算子，需要有action类算子触发执行
      *  2、对某一个算子执行cache算子后，需要将返回结果赋值给一个变量，在接下来的job直接使用这个变量那么就能够读取到缓存的数据
      *  3、如果想释放掉缓存到内存中的数据，使用unpersist方法，unpersist方法是以立即执行的算子
      */
    var rdd1 = sc.textFile("pom.xml")
    val startTime = System.currentTimeMillis()
    rdd1 = rdd1.cache()
    var count = rdd1.count()

    val endTime = System.currentTimeMillis()
    println("job0 durations:" + (endTime - startTime) + "\tresult:" + count)

    val startTime1 = System.currentTimeMillis()
    val count1 = rdd1.count()
    val endTime1 = System.currentTimeMillis()
    println("job1 durations:" + (endTime1 - startTime1) + "\tresult:" + count1)

    sc.stop()

  }

}
