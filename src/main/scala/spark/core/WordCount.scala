package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("woedcount")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/core/score.txt")

    val pairs = lines.flatMap(_.split(" ")).map((_,1))

    val wodCount = pairs.reduceByKey(_+_)


    // 排序 没有sortByValue算子。所以需要调换数据位置。
   // val wordSort = wodCount.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    val wordSort = wodCount.map(x=>(x._1,x._2.toInt)).sortBy(_._2,false)  // 此方法都是局部排序。

    //wordSort.collect()
    wordSort.foreach(println(_))
  }
}
