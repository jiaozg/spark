import org.apache.spark.{SparkConf, SparkContext}

object PiplelineTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PiplelineTest").setMaster("local")
    val sc = new SparkContext(conf)

    val nameRdd = sc.textFile("names")
    val mapRdd = nameRdd.map(x => {
      println( "map : " + x)
      x
    })

    val filterRdd = mapRdd.filter(x => {
      println("filter : " + x)
      true
    })

    filterRdd.count()

    sc.stop()
  }
}
