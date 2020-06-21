package spark.streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object KafkaDirectWordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(1))
    
//        val kafkaParams = Map[String, String](
//            "metadata.broker.list" -> "spark02:9092,spark03:9092"
//        )
        
        // 需要读取topic，可以并行读取多个topic
      //  val topics = Set[String]("WordCount")

//        val line: InputDStream[(String, String)] = KafkaUtils.
//          createDirectStream[String, String, StringDecoder, StringDecoder](
//            ssc,
//            kafkaParams,
//            topics
//        )


        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "Master:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "0001",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)//是否自动提交
        )
        val topics = Array("weblogs")
        val lines = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        
        val words = lines.flatMap(_.key().split(" "))
        
        val pairs = words.map((_, 1))
        
        val wordCounts = pairs.reduceByKey(_ + _)
        
        wordCounts.print()
        
        ssc.start()
        ssc.awaitTermination()
    }
}
