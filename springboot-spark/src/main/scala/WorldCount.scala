import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.setAppName("WorldCount")
    conf.setMaster("local")

    var sc = new SparkContext(conf)
    /**
      * lineRDD中每一行数据就是文本中每一条记录
      */
    val lineRdd = sc.textFile("README.md")
    /**
      * wordRDD 非kv格式的RDD  每一行数据就是一个单词
      */
    val wordRdd = lineRdd.flatMap(line => line.split(" "))
    /**
      * 将wordRDD 变成KV格式的RDD
      * pairRDD 每一行记录就是一个二元组类型的值（hello,1）
      */
    val pairRdd = wordRdd.map(word => (word, 1))
    /**
      * 按照key来分组，然后将每一组内的数据进行聚合
      * hello [1,1,1,1,1,1,1]
      *
      * 每一行数据
      *  （hello，count）
      *  （dilireba，count）
      */
    val resutRdd = pairRdd.reduceByKey((v1:Int, v2:Int) => {
      v1 + v2
    })
    resutRdd.foreach((x:(String, Int)) => {
      println("word: "+ x._1 + "\tcount: " + x._2)
    })

    /**
      * 释放资源
      */
    sc.stop()

  }

}
