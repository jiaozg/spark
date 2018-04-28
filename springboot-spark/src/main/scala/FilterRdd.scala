import org.apache.spark.{SparkConf, SparkContext}

object FilterRdd {

  def main(args: Array[String]): Unit = {
    //创建Spark运行时的配置对象，在配置对象里面可以设置APP name，集群URL以及运行时各种资源需求
    val sparkConf = new SparkConf().setAppName("MapOperator")
      .setMaster("local")

    //创建SparkContext上下文环境，通过传入配置对象实例化一个SparkContext
    val sc = new SparkContext(sparkConf)

    val dataSet = Array(1,215,324,9,245,56)

    val dataRdd = sc.parallelize(dataSet)

    val filterRdd =  dataRdd.filter (x=>{
      if(x > 200)
        true
      else {
        false
      }
    })

    val result = filterRdd.collect()

    for(record <- result){
      println(record)
    }

  }

}
