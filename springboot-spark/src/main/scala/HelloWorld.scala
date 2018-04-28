import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    //创建Spark运行时的配置对象，在配置对象里面可以设置APP name，集群URL以及运行时各种资源需求
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local");
    //创建SparkContext上下文环境，通过传入配置对象实例化一个SparkContext
    val sc = new SparkContext(conf)
    // setMaster指定Master

    val data = sc.textFile("README.md")
    // 加载README.md文件并创建RDD
    data.foreach(println)
    // 输出RDD中的每个分区的内容
  }
}
