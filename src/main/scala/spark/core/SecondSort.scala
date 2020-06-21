package spark.core



import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/22.
  */
object SecondSort {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("movies").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        val ratingsRDD = sc.textFile("./data/core/sort.txt")


        val pairWithSortKey=ratingsRDD.map(line=>{
            val splited=line.split("\t")
            (new MaySort(splited(0).toInt,splited(1).toInt,splited(2).toInt),line)
            //(new SecondarySortKey(splited(0).toDouble,splited(1).toDouble),line)
        })
        //直接调用sortBykey，此时会按照之前实现的compare方法排序
        val sorted=pairWithSortKey.sortByKey(false)
        val sortResult=sorted.map(sortedline=>sortedline._2).take(10)
        sortResult.foreach(println(_))

        sc.stop()

    }


}


class SecondarySortKey(val first:Double,val second:Double) extends  Ordered[SecondarySortKey] with  Serializable{
    //这个类中重写了compare方法
    override def compare(that: SecondarySortKey): Int = {
        //既然是二次排序，就先判断第一个字段是否相等，如果不相等，就直接排序
        if(this.first-that.first!=0){
            (this.first-that.first).toInt
        }else{
            //如果第一个字段相等，就比较第二个字段，若想要实现多次排序，也按照这个模式继续比较下去
            if(this.second-that.second>0){
                //ceil向上取整
                Math.ceil(this.second-that.second).toInt
            }else if(this.second-that.second<0){
                Math.floor(this.second-that.second).toInt
            }else{
                (this.second-that.second).toInt
            }
        }
    }
}



class MaySort(val first:Int,val second:Int,val third:Int) extends Ordered [MaySort] with Serializable {
    def compare(other:MaySort):Int = {
        if (this.first - other.first !=0) {
            this.first - other.first
        } else if (this.second - other.second !=0) {
            this.second - other.second
        } else {
            this.third - other.third
        }
    }
}