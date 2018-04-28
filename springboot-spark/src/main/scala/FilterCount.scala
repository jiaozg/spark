import org.apache.spark.{SparkConf, SparkContext}

object FilterCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FilterCount").setMaster("local")
    val sc = new SparkContext(conf)
    /**
      * RDD1中的数据：
      * 	2016-09-01	成都尚学堂	1124	北京
				2016-09-05	上海尚学堂	1125	广州
      */
    val rdd1 = sc.textFile("log.txt")
    /**
      * schoolname
      * schoolname
      * schoolname
      * schoolname
      * samples算子：Transformation类算子
      * 第一个参数:抽样的方式  true有返回的抽样
      * 第二个参数：抽样比例
      * 第三个参数：抽样算法的初始值
      */
    val rddSample = rdd1.sample(true, 0.8, 1)
    /**
      * 将每一个学校名   计数为1
      * rdd3就是一个KV格式的RDD
      * K：schoolname  V：1
      */
    val rdd2 = rddSample.map(_.split("\t")(1))
    /**
      * 将每一个学校名   计数为1
      * rdd3就是一个KV格式的RDD
      * K：schoolname  V：1
      */
    val rdd3 = rdd2.map((_,1))
    /**
      * 统计每一个学校出现的次数
      * schoolname [1,1,1,1,1,1]
      */
    val rdd4 = rdd3.reduceByKey((v1:Int, v2:Int) => v1 + v2)
    /**
      * sortBy x=>x._2  代表根据value来降序排序
      */
    val rdd6 = rdd4.sortBy((x:(String, Int)) => x._2, false)
    /**
      * 取出来RDD6中第一个元素，第一个元素就是出现次数最多的那个学校
      * Array[(Int, String)]
      * take:Action类算子
      * arr这个数组中只有1条记录   并且这一条记录是二元组类型的！！！
      */
    val arr = rdd6.take(1)
    val schoolName = arr(0)._1

    println("school name  = " + schoolName)

    /**
      * 出现次数最多的这个学校名有了，对rdd1进行过滤，在过滤的时候得依据schoolName
      */
    val resultRdd = rdd1.filter( x => {
      ! schoolName.equals(x.split("\t")(1))
    })
    /**
     * "result"是一个相对路径
     * saveAsTextFile:transformation类算子
     */
    resultRdd.saveAsTextFile("result")

    sc.stop()



  }
}
