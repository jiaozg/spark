import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器
  */
object AccumulatorTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorTest").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd = sc.textFile("pom.xml")
    //    var count = 0
    //    rdd.map(x=>{
    //      count += 1
    //    }).count
    //    println(count)
    val accumulator = sc.longAccumulator
    rdd.map(x => {
      accumulator.add(1)
    }).count()

    println(accumulator.value)

    sc.stop()


  }

}
